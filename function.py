from pandas import *
import json


def read_file(path):
    file_data = open(path,'r+')
    lines = file_data.readlines()
    return lines

def get_depara(path):
    file_json = open(path)
    return json.load(file_json)

def get_field_rules(path):
    xls = ExcelFile(path)
    dict_rules = dict()

    for sheets in range(len(xls.sheet_names)):
        df = xls.parse(xls.sheet_names[sheets])
        dict_rules[xls.sheet_names[sheets]] = df.to_dict('records')

    for sheets in xls.sheet_names:
        for line in dict_rules[sheets]:
            if not type(line['Nº ']) == int:
                dict_rules[sheets].remove(line)
    return dict_rules


def treat_line(line,rules):
    treated_line = dict()
    for rule in rules:
        treated_line[rule['CONTEÚDO '].rstrip()] = line[rule['INICIAL ']-1:rule['FINAL ']]
    return treated_line

def save_parquet(data_list,file_name):
    df = DataFrame.from_dict(data_list)
    new_file_name = file_name.split(".", 1)[0]
    df.to_parquet(f'data-parquet//{new_file_name}.parquet')
