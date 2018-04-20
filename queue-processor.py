"""
A ideia é propor uma solução que gere dados, processe eles (com uma certa taxa de erros), 
e escreva num arquivo de saída, utilizando uma fila com prioridades.
"""
from datetime import datetime
import random
import os
from queue import PriorityQueue

DEBUG = False
PAGE_COUNT = 5000
PROCESSOR_ERROR_RATE = 2
HIGH_PRIORITY = 1
OUTPUT_FOLDER = "Processed Data"
DATETIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
OUTPUT_FILE = DATETIME + '.txt'


def page_generator():
    """
    Generates data to be processed.

    """
    count = 0
    while True:
        if count == PAGE_COUNT:
            break
        yield count
        count += 1
    print('FINISHED (%d)' % count)


def page_processor(page, queue, *a, **kw):
    """
    Data processor.

    """
    if random.randint(0, 100) <= PROCESSOR_ERROR_RATE:
        if DEBUG:
            print('PROCESSOR: retry [%s]' % page)
        queue.put((HIGH_PRIORITY, page))
    else:
        if DEBUG:
            print('PROCESSOR: done [%s]' % page)
        priority = HIGH_PRIORITY + (queue.qsize() + 1)  # Always has lower priority
        queue.put((priority, page))


def output(page, *a, **kw):
    """
    Data output.

    """
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
    open(OUTPUT_FOLDER + '/' + OUTPUT_FILE, 'a').write('%s\n' % page)


def test():
    """
    Data consistency test.

    """
    data = open(OUTPUT_FOLDER + '/' + OUTPUT_FILE, 'r').readlines()
    assert len(data) == PAGE_COUNT
    assert sum(int(i) for i in data) == sum(range(PAGE_COUNT))


PAGES = page_generator()  # Calls generator object
QUEUE = PriorityQueue()  # Calls queue object

# Creating the queue 
for PAGE in PAGES:
    page_processor(PAGE, QUEUE)

# Saving the queue
while not QUEUE.empty():
    output(QUEUE.get()[1])

if __name__ == '__main__':
    test()
