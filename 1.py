import os
import xml.etree.cElementTree as ElementTree
import zipfile
from random import randint
from shutil import rmtree
from uuid import uuid4


def create_xml(xml_path: str):
    root = ElementTree.Element("root")
    ElementTree.SubElement(root, "var", name="id").text = str(uuid4())
    ElementTree.SubElement(root, "var", name="level").text = str(randint(1, 100))
    objects = ElementTree.SubElement(root, "objects")
    for items in range(randint(1, 10)):
        ElementTree.SubElement(objects, "object", name=str(uuid4()))
        ElementTree.dump(objects)
    ElementTree.ElementTree(root).write(xml_path)


def create_archives(zip_count: int, xml_count: int):
    os.mkdir('xml') if not os.path.exists('xml') else None
    for archive in range(zip_count):
        for xml in range(xml_count):
            create_xml(f'xml/{xml}.xml')

        os.mkdir('zip') if not os.path.exists('zip') else None

        with zipfile.ZipFile(f'zip/{archive}.zip', 'w') as zip_file:
            for item in range(xml_count):
                xml_path = f'xml/{item}.xml'
                zip_file.write(xml_path, f'{item}.xml')
    rmtree('xml')


if __name__ == '__main__':
    xml_files_count = 100
    arhive_files_count = 50
    create_archives(zip_count=arhive_files_count, xml_count=xml_files_count)









