
import random

DEBUG = False
PAGE_COUNT = 5000
PROCESSOR_ERROR_RATE = 2
HIGH_PRIORITY = 1
OUTPUT_FILE = 'output.txt'


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


def page_processor(page, next_step, queue, *a, **kw):
    """
    Data processor.

    """
    if random.randint(0, 100) <= PROCESSOR_ERROR_RATE:
        if DEBUG:
            print('PROCESSOR: retry [%s]' % page)
        queue.put((HIGH_PRIORITY, page))
    elif next_step:
        if DEBUG:
            print('PROCESSOR: done [%s]' % page)
        next_step.put(page)


def output(page, next_step, *a, **kw):
    """
    Data output.

    """
    open(OUTPUT_FILE, 'a').write('%s\n' % page)


def test():
    data = open(OUTPUT_FILE, 'r').readlines()
    assert len(data) == PAGE_COUNT
    assert sum(int(i) for i in data) == sum(range(PAGE_COUNT))


# TODO:
# link the generator, processor and output into a queue, such that the
# output file is filled with the generated data at the end.

QUEUE = None  #TODO


if __name__ == '__main__':
    test()