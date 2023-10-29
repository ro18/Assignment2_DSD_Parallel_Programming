"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
from threading import Thread
import pandas as pd
import time
from tqdm import tqdm

result=0

class ThreadingSolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, num_of_threads=None, dataset_path=None, dataset_size=None):
        self.num_of_threads = num_of_threads
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size



    def run(self):
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """
        start_time = time.time()

        def map_tasks(reading_info: list):
            global result 

            print(f"reading info:{reading_info}")
            df = pd.read_csv(self.dataset_path, nrows=reading_info[0], skiprows=reading_info[1], header=None)
            prefix = ['S','P']
            matched_rows=df.iloc[:,2].str.startswith(tuple(prefix))
            result+=matched_rows.tolist().count(True)

        
        def get_results(mapping_output: list):
            global result 

            for out in tqdm(mapping_output):
                result +=out
            return result
    
        def distribute_rows():
            reading_info = []
            chunk_size =  self.dataset_size // self.num_of_threads 
            skip_rows = 0
            for _ in range(self.num_of_threads):
                if skip_rows + chunk_size <= self.dataset_size :
                    reading_info.append([chunk_size, skip_rows])
                else:
                    reading_info.append([self.dataset_size  - skip_rows, skip_rows])
                skip_rows += chunk_size
            return reading_info
        



        chunk_distribution = distribute_rows()


        thread_handle = []

        for j in range(0, self.num_of_threads ):

            t = Thread(target=map_tasks,args=([chunk_distribution[j]]))
            thread_handle.append(t)

        for t in thread_handle:
            t.start()

        for j in range(0, self.num_of_threads):
            thread_handle[j].join()

        end_time = round(time.time() - start_time, 2)
   


        return result,end_time


if __name__ == '__main__':
    solution = ThreadingSolution(num_of_threads=4, dataset_path="implementation\Combined_Flights_2021.csv", dataset_size=6311871)
    answer, timetaken = solution.run()
    print(answer, timetaken)

