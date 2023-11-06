"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
# import threading
import math
import queue
from threading import Thread
import time
import pandas as pd

from tqdm import tqdm



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

        
        def reduce_task(mapping_output: dict, threads_worked:int):
            reduce_out = {}
            for out in tqdm(mapping_output):
                for key, value in out.items():

                    if key in reduce_out:

                        if not math.isnan(value):
                            reduce_out[key] = reduce_out.get(key) + value         
                    else:
                        reduce_out[key] = value
            

            for key, value in reduce_out.items():
                reduce_out[key] = value / threads_worked



            return max(reduce_out, key=reduce_out.get)
    

        start_time = time.time()

        def map_tasks(reading_info: list, result_queue: queue):

    
            df = pd.read_csv(self.dataset_path, nrows=reading_info[0], skiprows=reading_info[1], header=None)
            
            df['arr_delay_check'] = df.iloc[:,55] < 0


            df['Quarter'] = pd.PeriodIndex(df.iloc[:,0], freq='Q-DEC').strftime('Q%q')

            df2Quarter = df.loc[df['Quarter'] == "Q1"]


            if not df2Quarter.empty:
                result = (
                df2Quarter.groupby(df2Quarter.iloc[:,1]).apply(lambda x : pd.Series({
                    'arrive_early_per':  ( x['arr_delay_check'].sum() / df2Quarter['Quarter'].count() ) * 100

                }))
                .reset_index()
                )


                result_queue.put(result.set_index(1)['arrive_early_per'].to_dict())


    
        def distribute_rows():
            reading_info = []
            chunk_size =  self.dataset_size // self.num_of_threads 
            skip_rows = 1
            for _ in range(self.num_of_threads):
                if _ != self.num_of_threads-1:
                    reading_info.append([chunk_size, skip_rows])
                else:
                    reading_info.append([self.dataset_size - skip_rows, skip_rows])
                skip_rows += chunk_size

                       
            return reading_info
        



        chunk_distribution = distribute_rows()

       

        thread_handle = []

        result_queue = queue.Queue()

        for j in range(0, self.num_of_threads ):

            t = Thread(target=map_tasks,args=(chunk_distribution[j],result_queue))
            thread_handle.append(t)

        for t in thread_handle:
            t.start()


        results = []
        threads_worked= 0

        for j in range(0, self.num_of_threads):
            thread_handle[j].join()
            if not result_queue.empty():
                threads_worked+=1
                result = result_queue.get()

                results.append(result)


        final_result = reduce_task(results,threads_worked)



        end_time = round(time.time() - start_time, 2)
   

        return (final_result,end_time)



if __name__ == '__main__':
    #solution = ThreadingSolution(num_of_threads=4, dataset_path="Test3.csv", dataset_size=4)

    solution = ThreadingSolution(num_of_threads=4, dataset_path="implementation/Combined_Flights_2021.csv", dataset_size=6311871)
    answer, timetaken = solution.run()
    print(answer, timetaken)

