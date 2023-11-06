"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import itertools
import math
from threading import Thread
import pandas as pd
import time
from tqdm import tqdm
from pathos.multiprocessing import Pool

class MultiProcessingSolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, num_of_processes=None, dataset_path=None, dataset_size=None):
        self.num_of_processes = num_of_processes
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self):
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """

        start = time.time()


        def reduce_task(mapping_output: dict):
            reduce_out = {}

            for out in tqdm(mapping_output):
                for key, value in out.items():

                    if key in reduce_out:
                        if not math.isnan(value):
                            reduce_out[key] = reduce_out.get(key) + value
                    else:
                        reduce_out[key] = value



            for key, value in reduce_out.items():
                reduce_out[key]= value / self.num_of_processes




            return max(reduce_out, key=reduce_out.get)

        def distribute_rows(n_rows: int, n_processes):
            reading_info = []
            chunk_size =  n_rows // n_processes
            skip_rows = 1
            for _ in range(n_processes):
                if _ != n_processes-1:
                    reading_info.append([chunk_size, skip_rows])
                else:
                    reading_info.append([n_rows - skip_rows, skip_rows])
                skip_rows += chunk_size
                        
            return reading_info
        

        p = Pool(processes=self.num_of_processes)
        
        def map_tasks(reading_info: list,data : str ):      

            df = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None, index_col=False)

            df['on_time'] = df.iloc[:,11]

            on_time = df['on_time'].tolist().count(0)

            result = (
            df.groupby(df.iloc[:,1]).apply(lambda x : pd.Series({
                'onTime_Percentage':  (x['on_time'].eq(0).sum() / on_time ) * 100

            }))
            .reset_index()
            )







            return result.set_index(1)['onTime_Percentage'].to_dict()
        

        result = p.starmap(map_tasks, zip(distribute_rows(n_rows=self.dataset_size, n_processes=self.num_of_processes),itertools.repeat(self.dataset_path)))


        final_result = reduce_task(result)
        p.close()
        p.join()

        return final_result, round(time.time()-start,2)



if __name__ == '__main__':
    solution = MultiProcessingSolution(num_of_processes=4, dataset_path="implementation/Combined_Flights_2021.csv", dataset_size=6311871)
    result, timetaken = solution.run()
    print(result, timetaken)

