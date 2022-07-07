import csv
import multiprocessing
import os
import xml.etree.cElementTree as ElementTree
from zipfile import ZipFile
from datetime import datetime


def parser(count_xml_files, start, stop, mp_queue):
    for archive_number in range(start, stop):
        os.mkdir('unzip') if not os.path.exists('unzip') else None
        
        with ZipFile(f'zip/{archive_number}.zip', 'r') as zip_file:
            unzip_path = f'unzip/{archive_number}'
            os.mkdir(unzip_path) if not os.path.exists(unzip_path) else None
            zip_file.extractall(unzip_path)
        
        for xml in range(count_xml_files):
            root_node = ElementTree.parse(f'{f"unzip/{archive_number}"}/{xml}.xml').getroot()
            id = ''
            level = ''
            
            for tag in root_node.findall('var'): 
                tag_type = tag.get('name')
                tag_value = tag.text
                if tag_type == 'id': id = tag_value
                if tag_type == 'level': level = tag_value
            mp_queue.put({'id_level': [id, level]})
            
            for tag in root_node.findall('objects/object'):
                mp_queue.put({'id_object': [id, tag.get('name')]})


def csv_writer_queue_listner(mp_queue):
    with open('id_level.csv', 'w', newline='') as csv_id_level_file:
        with open('id_object.csv', 'w', newline='') as id_object_file:
            wr_id_object = csv.writer(id_object_file, quoting=csv.QUOTE_ALL)
            wr_id_level = csv.writer(csv_id_level_file, quoting=csv.QUOTE_ALL)
            
            while True:
                item = mp_queue.get()
                if item == 'kill': break
                if 'id_level' in item.keys(): wr_id_level.writerow(item['id_level'])
                if 'id_object' in item.keys(): wr_id_object.writerow(item['id_object'])


if __name__ == '__main__':
    count_xml_files = 100
    count_arhive_files = 50
    processes_count = 5

    start_time = datetime.now()
    manager = multiprocessing.Manager()
    processes = []
    mp_queue = manager.Queue()
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    watcher = pool.apply_async(csv_writer_queue_listner, (mp_queue,))

    start_point = 0
    stop_point = int(count_arhive_files/processes_count)

    for process in range(processes_count):
        proc = pool.apply_async(parser, (count_xml_files, start_point, stop_point, mp_queue))
        processes.append(proc)
        start_point = stop_point
        stop_point = stop_point + int(count_arhive_files/processes_count)

    for process in processes:
        process.get()

    mp_queue.put('kill')
    pool.close()
    pool.join()
    
    print("processing time:", datetime.now() - start_time)






