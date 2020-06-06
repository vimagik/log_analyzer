#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

import os
import gzip
import configparser
import logging
import argparse
import re
from string import Template
from collections import namedtuple


def initLogging(log_dir):
    """
    Инициализируем логирование
    """
    logging.basicConfig(
        filename=os.path.join(log_dir, "log_analizer.log"),
        level=logging.INFO,
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S'
    )


def parse_config():
    """
    Читаем конфиг по переданному пути (./config - адрес по-умолчанию)
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default="./config")
    console_data = parser.parse_args()
    if not os.path.exists(os.path.join(console_data.config, 'config.ini')):
        raise FileNotFoundError
    config = configparser.ConfigParser()
    config.read(os.path.join(console_data.config, 'config.ini'))
    if 'log_analyzer' in config:
        config = {
            "REPORT_SIZE": config.get('log_analyzer', 'REPORT_SIZE', fallback='1000'),
            "REPORT_DIR": config.get('log_analyzer', 'REPORT_DIR', fallback='./reports'),
            "LOG_DIR": config.get('log_analyzer', 'LOG_DIR', fallback='./log'),
            "PROG_LOG_DIR": config.get('log_analyzer', 'PROG_LOG_DIR', fallback='.'),
            "ERROR_LEVEL": config.get('log_analyzer', 'ERROR_LEVEL', fallback='20'),
        }
        return config


def search_log(log_dir):
    """
    Ищет в папке log_dir последний актуальный лог,
    возвращает адрес лога и его дату
    """
    if not os.path.exists(log_dir):
        raise FileNotFoundError
    max_date = "00000000"
    for file in os.listdir(path=log_dir):
        file_match = re.match(r"nginx-access-ui\.log-(\d{8})", file)
        if file_match:
            if file_match.group(1) >= max_date:
                max_date = file_match.group(1)
                filename = file
    Log = namedtuple('Log', 'file date')
    new_log = Log(os.path.join(log_dir, filename), max_date)
    return new_log


def extract_log(log_data):
    """
    Парсер для файла с логом. Возвращает url и время запроса
    """
    log_data_str = str(log_data, 'utf-8')
    request_time = float(log_data_str.split(' ')[-1])
    start_url = log_data_str.find('"') + 1
    end_url = log_data_str.find('"', start_url)
    try:
        reques_url = log_data_str[start_url:end_url].split(" ")[1]
    except IndexError:
        reques_url = "url_error"
    return [reques_url, request_time]


def mediana(list):
    """
    Нахождение медианы в листе
    """
    data = sorted(list)
    n = len(data)
    if n == 0:
        return None
    if n % 2 == 1:
        return data[n // 2]
    else:
        i = n // 2
        return (data[i - 1] + data[i]) / 2


def save_logs(log_stat, report_full_name):
    """
        Сохраняет подготовленную статистику в шаблонный отчет в папку report_dir
    """
    if not os.path.exists("report.html"):
        logging.error("No such file or directory: 'report.html'")
        raise FileNotFoundError
    with open("report.html", "r") as f:
        html_data = Template(f.read()).safe_substitute(table_json=log_stat)
        if not os.path.exists(os.path.dirname(report_full_name)):
            logging.error(f"No such directory: {os.path.dirname(report_full_name)}")
            raise FileNotFoundError
        with open(report_full_name, "w") as f_out:
            f_out.write(html_data)


def create_report(log_stat, report_size, count_all, time_all):
    """
    На основе агрегированных данных логов строит отчет, находит все метрики
    """
    log_stat_list = list(log_stat.items())
    log_stat_list.sort(key=lambda i: i[1]["time_sum"], reverse=True)
    i = 0
    report = []
    while i < report_size:
        log = {
            "url": log_stat_list[i][0],
            "count": log_stat_list[i][1]["count"],
            "count_perc": log_stat_list[i][1]["count"] / count_all * 100,
            "time_sum": log_stat_list[i][1]["time_sum"],
            "time_perc": log_stat_list[i][1]["time_sum"] / time_all * 100,
            "time_avg": log_stat_list[i][1]["time_sum"] / log_stat_list[i][1]["count"],
            "time_max": max(log_stat_list[i][1]["values"]),
            "time_med": mediana(log_stat_list[i][1]["values"]),

        }
        report.append(log)
        i += 1
    return report


def agregate_stat(logs):
    """
    Агрегирует логи, на выходе словарь с данными по каждому url,
    суммарное количество и суммарное время
    """
    log_stat = {}
    count_all = 0
    time_all = 0
    for log in logs:
        if log[0] not in log_stat:
            log_stat[log[0]] = {
                "count": 0,
                "time_sum": 0,
                "values": [],
            }
        log_stat[log[0]]["count"] += 1
        log_stat[log[0]]["time_sum"] += log[1]
        log_stat[log[0]]["values"].append(log[1])
        count_all += 1
        time_all += log[1]
    return log_stat, count_all, time_all


def read_logs(file_adress):
    """
    Читает файл с логами
    """
    _, file_format = os.path.splitext(file_adress)
    func = gzip.open if file_format == '.gz' else open
    with func(file_adress, "rb") as f:
        for line in f:
            yield line


def main():
    config = parse_config()
    initLogging(config["PROG_LOG_DIR"])
    logging.info("Program started")
    try:
        report_size = int(config["REPORT_SIZE"])
        error_level = float(config["ERROR_LEVEL"])
    except ValueError:
        logging.error("Config error. Report size or error level are not a digit")
        return
    new_log = search_log(config["LOG_DIR"])
    logging.info("Log file found")
    report_name = f"report-{new_log.date[:4:]}.{new_log.date[4:6:]}.{new_log.date[6::]}.html"
    report_full_name = os.path.join(config["REPORT_DIR"], report_name)
    if os.path.exists(report_full_name):
        logging.info("Done! This log already processed")
        return
    logs = map(extract_log, read_logs(new_log.file))
    log_stat, count_all, time_all = agregate_stat(logs)
    error_per = log_stat["url_error"]["count"] / count_all * 100
    if error_per >= error_level:
        logging.error("Error rate is too high: ", error_per)
        return
    logging.info("All logs agregated")
    report = create_report(
        log_stat, report_size,
        count_all, time_all
    )
    logging.info("The report created")
    save_logs(report, report_full_name)
    logging.info("Done!")


if __name__ == "__main__":
    main()
