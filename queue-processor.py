"""
A ideia é propor uma solução que gere dados, processe eles (com uma certa taxa 
de erros), e escreva num arquivo de saída, utilizando uma fila com prioridades.
Requisito 1:
- Permitir utilizar uma sequencia de `page_processor`s, não apenas 1 
(podem ser outras funções, com a mesma assinatura)
- O `next_step é pensado para poder acoplar novos passos
- Permitir execução paralela dessa sequencia de processors
- Bonus: utilizar async/await ou gevent para isso
Requisito 2:
- Não manter mais de N itens por vez em memória na fila, para controlar uso de 
recursos da maquina.

"""
import random
import os
from datetime import datetime
import gevent
from gevent.queue import PriorityQueue, Empty
import inspect

DEBUG = True
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
    print('###### DATA INPUT HAS STOPPED AFTER %d PAGES ######' % count)


def page_processor(name, pages, next_step, queue, *a, **kw):
    """
    Processes input data and adds it to queue.
    
    Parameters
    ----------
    name : string
        Processor name
    pages : generator object
        Input data
    next_step : function
        Subsequent function
    queue : queue object
        Queue that receives data
        
    Returns
    ----------
    None

    """
    for page in pages:
        while queue.full():
            print('Queue is busy! %s is on hold...' % name)
            gevent.sleep(random.randint(0, 8))
        if DEBUG:
            time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('@ %s  %s added page %s' % (time, name, page))
        if random.randint(0, 100) <= PROCESSOR_ERROR_RATE:
            if DEBUG:
                print('PROCESSOR: retry [%s]' % page)
            queue.put((HIGH_PRIORITY,page),timeout=30)
            gevent.sleep(0) # Set >0 to simulate processor delay 
        elif next_step:
            if DEBUG:
                print('PROCESSOR: done [%s]' % page)
            priority = HIGH_PRIORITY + (queue.qsize() + 1) 
            queue.put((priority,page),timeout=30)
            gevent.sleep(0) # Set >0 to simulate processor delay 
    print('############### %s has quit! ###############' % name)


def pick_page(name, pages, queue, *a, **kw):
    """
    Picks the highest priority page from the queue.
    
    Parameters
    ----------
    name : string
        Processor name
    pages : generator object
        Input data
    queue : queue object
        Queue with data to be picked
        
    Returns
    ----------
    None
    
    """
    while True:
        try:
            while True:
                __, page = queue.get(timeout=30) 
                output(page, next_step(queue))
                if DEBUG:
                    time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print('@ %s  %s got page %s' % (time, name, page))
                gevent.sleep(0) # Set >0 to simulate picker delay 
        except Empty:
            if DEBUG:
                print('Queue is empty! %s is on hold...' % name)
            # Adding time delay in case any more data is generated
            gevent.sleep(random.randint(4,8))
            if queue.empty() and inspect.getgeneratorstate(pages) =='GEN_CLOSED':
                print('################ %s has quit! ###############' % name)
                break
    
        
def output(page, next_step, *a, **kw):
    """
    Outputs data.

    """
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
    open(OUTPUT_FOLDER + '/' + OUTPUT_FILE, 'a').write('%s\n' % page)
    
    
def next_step(queue):
    """
    Do something.
    
    """
    return queue


def test():
    """
    Tests consistency of output data.

    """
    data = open(OUTPUT_FOLDER + '/' + OUTPUT_FILE, 'r').readlines()
    assert len(data) == PAGE_COUNT
    assert sum(int(i) for i in data) == sum(range(PAGE_COUNT))
    
    
def main(n_processor = 3, n_picker = 3, queue_size = 10, *a, **kw):
    """
    Queue processor simulator.
    
    Parameters
    ----------
    n_processor : int
        Number of processors working simultaneously
    n_picker : int
        Number of pickers working simultaneously
    queue_size : int
        Maximum allowed size of queue
        
    Returns
    ----------
    None

    """
    pages = page_generator()
    global queue
    queue = PriorityQueue(maxsize = queue_size)
    spawn_list = []
    for i in range(n_processor):
        greenlet = gevent.spawn(page_processor, 'Processor {0}'.format(i+1), pages, next_step, queue)
        spawn_list.append(greenlet)
    for j in range(n_picker):
        greenlet = gevent.spawn(pick_page, 'Picker {0}'.format(j+1), pages, queue)
        spawn_list.append(greenlet)
    gevent.joinall(spawn_list)
    print('####################### END #######################')
    
          
if __name__ == '__main__':
    import time 
    srt_time = time.time()
    main()
    try:
        test()
        print("Code ran sucessfully after %.3f seconds!" % (time.time() - srt_time))
    except AssertionError as error: 
        if DEBUG:
            import logging
            time.sleep(1)
            logging.exception(error, 'Output data is not consistent with input data!')
        else:
            print("Code crashed!!! Output data is not consistent with input data!" )