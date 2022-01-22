from function import read_file, get_field_rules, treat_line, get_depara, save_parquet

de_para = get_depara('config//de_para.json')
for file in de_para.keys():
    treated_lines = list()
    file_lines = read_file(f'data//{file}')
    all_rules = get_field_rules('config//Convenio 115 - Regra de campos.xlsx')
    for line in file_lines:
        treated_lines.append(treat_line(line, all_rules[de_para[file]]))
    save_parquet(treated_lines, file)