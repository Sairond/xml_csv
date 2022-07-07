import csv
import os
import xml.etree.cElementTree as ElementTree
import zipfile
from datetime import datetime


def csv_parser():
    with open('id_level.csv', 'w', newline='') as csv_id_level_file:
        with open('id_object.csv', 'w', newline='') as id_object_file:
            wr_id_object = csv.writer(id_object_file, quoting=csv.QUOTE_ALL)
            wr_id_level = csv.writer(csv_id_level_file, quoting=csv.QUOTE_ALL)
            for archive_number in range(count_arhive_files):
                os.mkdir('unzip') if not os.path.exists('unzip') else None
                with zipfile.ZipFile(f'zip/{archive_number}.zip', 'r') as zip_file_obj:
                    unzip_path = f'unzip/{archive_number}'
                    os.mkdir(unzip_path) if not os.path.exists(unzip_path) else None
                    zip_file_obj.extractall(unzip_path)
                xml_dir_path = f'unzip/{archive_number}'
                for xml in range(count_xml_files):
                    root_node = ElementTree.parse(f'{xml_dir_path}/{xml}.xml').getroot()
                    id = ''
                    level = ''
                    for tag in root_node.findall('var'):
                        tag_type = tag.get('name')
                        tag_value = tag.text
                        if tag_type == 'id': id = tag_value
                        if tag_type == 'level': level = tag_value
                    wr_id_level.writerow([id, level])
                    for tag in root_node.findall('objects/object'):
                        wr_id_object.writerow([id, tag.get('name')])


if __name__ == '__main__':
    count_xml_files = 100
    count_arhive_files = 50
    start_time = datetime.now()
    csv_parser()
    print("Parsing time:", datetime.now() - start_time)






