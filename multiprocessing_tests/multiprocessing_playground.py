import time
from multiprocessing import get_context
import threading
import queue
import os
import multiprocessing


class SimpleContext:
    @staticmethod
    def write(key, value):
        return (key, value)

class Mapper:
    def map(self, key: str, value: str, context: SimpleContext):
        a = 0
        #Do a fake operation on the value and the key
        for i in range(10):
            a += i
        return context.write(key, value)

def _load_data_with_shared_queue(chunk_size):
    #I loaded data in multiple queue because each queue only holds about 30000 some lines
    print("Loading data...")
    in_queues = []
    in_queue = None
    manager = multiprocessing.Manager()
    line_count = 0
    with open("../inputfiles/numbers.text") as file:
        for i, line in enumerate(file):
            if i % (chunk_size-1) == 0:
                #If the queue reached the chunk size
                if i != 0:
                    in_queue.put("DONE")
                #Create a new queue once reaching the chunk size
                in_queue = manager.Queue()
                in_queues.append(in_queue)
            in_queue.put(line)
            line_count += 1
        in_queue.put("DONE")

    #Check
    for i in range(len(in_queues)):
        print("Loaded data in Queue"+str(i)+": Size="+str(in_queues[i].qsize()))

    return in_queues, line_count


def _load_data_with_normal_queue():
    global_queue = queue.Queue()
    line_count = 0
    print("Loading data...")
    with open("../inputfiles/numbers.text", "r") as file:
        line = file.readline()
        while line != "":
            global_queue.put(line)
            line = file.readline()
            line_count+=1

    print("Loaded Line count:", line_count)
    global_queue.put("DONE")
    return global_queue, line_count


def map_thread_func_for_process(index, in_queue, out_queue, mapper_class):
    key = "key"
    mapper = mapper_class()
    context = SimpleContext()
    value = ""
    done = False
    print("Thread"+str(os.getpid()))
    while not done:
        value = in_queue.get()
        # print(str(os.getpid())+":"+value)
        if value == "DONE":
            done = True
            out_queue.put("DONE")
            print("DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        else:
            out_queue.put(mapper.map(key, value, context))

def map_thread_func_for_pool(args):
    key = str(os.getpid())
    mapper = args["mapper_class"]()
    context = SimpleContext()
    in_queue = args["in_queue"]
    out_queue = args["out_queue"]
    value = ""
    done = False
    while not done:
        value = in_queue.get()
        if value == "DONE":
            done = True
            out_queue.put("DONE")
            print("Process finished:"+key)
        else:
            out_queue.put(mapper.map(key, value, context))

def run_with_process():
    threads = []
    in_queue_set, original_line_count = _load_data_with_shared_queue(chunk_size=10000)
    out_queue = multiprocessing.Queue()

    #Measure the execution time
    print("Processing...")
    start = time.time()
    for i in range(len(in_queue_set)):
        threads.append(multiprocessing.Process(target=map_thread_func_for_process, args=(i, in_queue_set[i], out_queue, Mapper)))
        threads[i].start()
    print("     - Execution time: {:.10f}".format(time.time() - start) + " seconds")
    for i in range(len(threads)):
        print("Joining..", str(i))
        threads[i].join()
    line_count = check_results(out_queue)
    end_time = time.time() - start
    print("     - Execution time: {:.10f}".format(end_time) + " seconds")
    print("Retrieved item Count:", line_count)
    print("Original item Count:", original_line_count)



def run_with_pool():
    in_queue_set, original_line_count = _load_data_with_shared_queue(chunk_size=10000)
    out_queue = multiprocessing.Manager().Queue()

    #arg_set
    args_set = []
    for i in range(len(in_queue_set)):
        args = {"mapper_class": Mapper,
                   "in_queue": in_queue_set[i], "out_queue": out_queue}
        args_set.append(args)

    print("Processing...")
    start = time.time()
    with multiprocessing.Pool(os.cpu_count()) as pool:
        pool.map(map_thread_func_for_pool, args_set)
    print("     - Execution time: {:.10f}".format(time.time() - start) + " seconds")

    line_count = check_results(out_queue)
    print("Retrieved item Count:", line_count)
    print("Original item Count:", original_line_count)


def run_with_single_thread():
    in_queue, original_count = _load_data_with_normal_queue()
    out_queue = queue.Queue()

    #Measure the execution time
    print("Processing...")
    start = time.time()
    map_thread_func_for_process(0, in_queue, out_queue, Mapper)
    print("     - Execution time: {:.10f}".format(time.time() - start) + " seconds")
    line_count = check_results(out_queue)
    print("Retrieved item Count:", line_count)
    print("Original item Count:", original_count)


def check_results(out_queue):
    print("---results---")
    line_count = 0
    out_value = ""
    done = False
    while not out_queue.empty():
        out_value = out_queue.get()
        #print("output:", out_value)
        line_count += 1
        # if out_value == "DONE":
        #     done = True
    print("Emptied out_queue")
    return line_count

if __name__ == '__main__':
    print("run_with_pool()")
    run_with_pool()
    print("----------------------------------------")
    # print("run_with_process()")
    # run_with_process()
    # print("----------------------------------------")
    print("run_with_single_thread()")
    run_with_single_thread()
