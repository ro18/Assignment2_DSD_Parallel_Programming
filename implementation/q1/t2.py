"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import itertools
from threading import Thread
import pandas as pd
import time
from tqdm import tqdm
from multiprocessing import Pool

def map_tasks(reading_info: list,data : str ):
    df = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None)
    prefix = ['S','P']
    matched_rows=df.iloc[:,2].str.startswith(tuple(prefix))
    return matched_rows.tolist().count(True)
    


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

        def reduce_task(mapping_output: list):
            reduce_out = 0
            for out in tqdm(mapping_output):
                reduce_out +=out

            return reduce_out

        def distribute_rows(n_rows: int, n_processes):
            reading_info = []
            chunk_size = n_rows // n_processes
            skip_rows = 0
            for _ in range(n_processes):
                if skip_rows + chunk_size <= n_rows:
                    reading_info.append([chunk_size, skip_rows])
                else:
                    reading_info.append([n_rows - skip_rows, skip_rows])
                skip_rows += chunk_size
            return reading_info

        #print(distribute_rows(n_rows=self.dataset_size, n_processes=self.num_of_processes))

        p = Pool(processes=self.num_of_processes)

        result = p.starmap(map_tasks, zip(distribute_rows(n_rows=self.dataset_size, n_processes=self.num_of_processes),itertools.repeat(self.dataset_path)))
        print(result)
        final_result = reduce_task(result)
        p.close()
        p.join()

        return final_result, round(time.time()-start,2)




if __name__ == '__main__':
    solution = MultiProcessingSolution(num_of_processes=4, dataset_path="implementation\Combined_Flights_2021.csv", dataset_size=6311871)
    result, timetaken = solution.run()
    print(result, timetaken)

