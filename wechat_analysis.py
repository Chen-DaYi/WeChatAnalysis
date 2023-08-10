import requests
import re
import json
import schedule
import time
import logging
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from modules.wechat import WeChat
from modules.claude_api import Client

load_dotenv()
cookie = os.getenv('COOKIE')
error_user = os.getenv('ERROR_USER')
user_id = os.getenv('ANALYSIS_USER')
retry_count = 0
logging.basicConfig(filename='./wechat.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().setLevel(logging.INFO)

time_ranges = [
    (0, 11.5, '上午'),
    (11.5, 17.5, '下午'),
    (17.5, 22.5, '晚间'),
    
]

wechat = WeChat()


def get_prompt(time_period):
    prompt = """两个任务：
1. 请总结一下主要聊天内容，帮助没有参与的人也能知晓都聊了什么。内容尽量详细，包含关键发言以及发言人。
2. 根据这份聊天记录，分析谁最有可能是Gay，以及推理原因和依据。
csv文件是一个微信群一天的聊天记录，格式如下：
---begin
每行是一个人的一次发言
每行以制表符\t分割
每行2个字段，以此是发言人、发言内容
---end"""

    prompt_summary = f"""请总结一下主要聊天内容，帮助没有参与的人也能知晓都聊了什么。内容尽量详细，包含关键发言以及发言人。以“下面播报今日{time_period}不能错过的重大事项：”为开头进行回答。
csv文件是一个微信群一天的聊天记录，格式如下：
---begin
每行是一个人的一次发言
每行以制表符\t分割
第一列是发言人，第二列是发言内容
---end"""

    prompt_gay = """根据这份聊天记录，分析谁最有可能是Gay，以及推理原因和依据。
回复去掉“我认为”、“我觉得”等主观性词语，去掉为了凑字数而无意义的词语，只保留推理原因和依据，去掉为了显示局限性而加的额外说明内容。
以“我认为XXX最有可能是Gay，因为XXX”为开头进行回答。
---begin
每行是一个人的一次发言
每行以制表符\t分割
第一列是发言人，第二列是发言内容
---end"""

    # prompt_gay = """根据这份聊天记录，分析谁最有可能是Gay，以及推理原因和依据。"""
    return prompt_summary, prompt_gay


def get_now_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def clean_today_msg(user_id, file_path=None, is_save=True):
    r_json = requests.get(wechat.CHAT_LOG_URL, params={'userId': user_id, 'count': 10000}).json()[1:]
    # 筛选文本信息
    msg_select = [msg for msg in r_json 
                if not re.search(r'\[该消息类型暂不能展示\]', msg['title'])
                    and len(msg['subTitle']) == 8
                    and '：' in msg['title'] 
                    ]
    
    msg_select = [[msg['subTitle'], msg['title'].split('：', 1)[0], msg['title'].split('：', 1)[1]]
                  for msg in msg_select]
    msg_select_df = pd.DataFrame(msg_select, columns=['time', 'user', 'send_text']).sort_values(by='time').reset_index(drop=True)
    msg_select_df['send_text'] = msg_select_df['send_text'].str.replace('\t', ' ').replace('\n', '。')
    if is_save and file_path:
        msg_select_df[['user', 'send_text']].to_csv(file_path, index=False, sep='\t')
    msg_select_df = msg_select_df.loc[msg_select_df['user'].map(lambda x: x != '芝士夹心饼干')] # type: ignore
    date_prefix = datetime.now().strftime('%Y-%m-%d ')
    msg_select_df['time'] = date_prefix + msg_select_df['time']

    return msg_select_df

def get_period():
    now = datetime.now()
    current_hour = now.hour + now.minute/60
    for time_start, time_end, time_period in time_ranges:
        if time_start <= current_hour < time_end:
            break
        next_time_start, next_time_end, next_time_period = time_start, time_end, time_period
    time_start, time_end, time_period = next_time_start, next_time_end, next_time_period
    return time_start, time_end, time_period


def select_analysis_msg(user_id, file_path, time_start, time_period):
    msg_select_df = clean_today_msg(user_id, file_path)
    logging.info(f'\tSum of msg: {msg_select_df.shape[0]}')

    # 生成ask_claude的输入
    
    msg_select_df['time'] = pd.to_datetime(msg_select_df['time'])
    msg_select_df['decimal_time'] = msg_select_df['time'].dt.hour + msg_select_df['time'].dt.minute/60  
    msg_select_df = msg_select_df[msg_select_df['decimal_time'] >= time_start]
    file_content = '\n'.join(msg_select_df['user'].str.cat(msg_select_df['send_text'], sep='\t')) # type: ignore
    # file_content = '\n'.join(msg_select_df.apply(lambda x: f"{x['time']}\t{x['user']}\t{x['send_text']}", axis=1)) # type: ignore

    logging.info(f'\tSum of msg - select: {msg_select_df.shape[0]}')    
    return msg_select_df, file_content


def ask_claude(prompts, file_path, file_content):
    # 创建会话
    claude_api = Client(cookie)
    conversation_id = claude_api.create_new_chat()['uuid']

    # 发送消息
    answers = []
    for i ,prompt in enumerate(prompts):
        if i ==0:
            answer = claude_api.send_message_withfilecontent(prompt, conversation_id, file_path=file_path, file_content=file_content)
        else:
            answer = claude_api.send_message_withfilecontent(prompt, conversation_id)
        logging.info(f'\t\t{"Succeeded to get Answer" if answer else "Failed to get Answer"} - No.{i} Prompt')
        answers.append(answer) if answer else None
    
    # 删除会话
    deleted = claude_api.delete_conversation(conversation_id)
    logging.info(f'\t\t{"Succeeded to delete tConversation" if deleted else "Failed delete tConversation"}')
    return answers


def wechat_analysis(user_id):
    
    # 获取当前时间段
    nowdate = datetime.now().strftime('%Y%m%d')
    file_path = f'./data/clean_today_{user_id}_{nowdate}.csv'
    time_start, _, time_period = get_period()
    logging.info(f'{time_period}\t' + '-*-'*12)
    
    # 准备输入
    msg_select_df, file_content = select_analysis_msg(user_id, file_path, time_start, time_period)
    prompt_summary, prompt_gay = get_prompt(time_period)
    
    # 提问
    logging.info('\tAsk Claude ...')
    answers = ask_claude([prompt_summary, prompt_gay], file_path, file_content)
    
    # 发送信息
    text_send = f'{time_period}分析聊天记录数：{len(msg_select_df)}'
    logging.info(f'\t{ "Sent successfully" if wechat.send_message_by_ids([user_id], text_send) else "Failed to send"} - headtext')
    logging.info(f'\t{ "Sent successfully" if wechat.send_message_by_ids([user_id], answers[0]) else "Failed to send"} - answer_summary')
    logging.info(f'\t{ "Sent successfully" if wechat.send_message_by_ids([user_id], answers[1]) else "Failed to send"} - answer_gay')

def job_wechat_analysis():
    global retry_count
    try:
        wechat_analysis(user_id)
        retry_count = 0
    except Exception as e:
        logging.error(e)
        if error_user:
            wechat.send_message_by_ids([error_user], f'分析失败，请手动分析 - {retry_count}次')
            wechat.send_message_by_ids([error_user], e)
            retry_count += 1
            if retry_count > 3:
                wechat.send_message_by_ids([error_user], '!!! 重试超过3次，停止任务 !!!')
                return # 如果重试超过3次,停止任务
            time.sleep(30)
        raise e

if __name__ == '__main__':
    # 手动找到要分析的userId
    # wechat = WeChat()
    # wechat.search_user_by_keyword('Apple') 
    # wechat.search_user_by_keyword('Sevn')
    schedule.every().day.at('11:30').do(job_wechat_analysis)
    schedule.every().day.at('17:30').do(job_wechat_analysis)
    schedule.every().day.at('22:30').do(job_wechat_analysis)
    logging.info('Start Analysis')
    while True:
        schedule.run_pending()
        time.sleep(10)