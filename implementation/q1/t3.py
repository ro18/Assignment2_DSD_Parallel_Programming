"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""

from os import system
import sys
import time
import math
import pandas as pd
from mpi4py import MPI
import pandas as pd
import time
from tqdm import tqdm

class MPISolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, dataset_path=None, dataset_size=None):
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size
        self.process_size=4


    

    def run(self):
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """

        final_results=""

        def reduce_task(mapping_output: dict, size:int):
            reduce_out = {}

            for out in tqdm(mapping_output):
                for key, value in out.items():
                    if key in reduce_out:
                        if not math.isnan(value):
                            reduce_out[key] = reduce_out.get(key) + value
                    else:
                        reduce_out[key] = value


            for key, value in reduce_out.items():        
                reduce_out[key] = value/(size-1)


            return max(reduce_out, key=reduce_out.get)
        
        
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()

        results = []
        start = time.time()
        if rank == 0:
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

            slave_workers = size - 1
            chunk_distribution = distribute_rows(n_rows=self.dataset_size, n_processes=slave_workers)

            
            for worker in range(1, size):
                chunk_to_process = worker-1
                comm.send(chunk_distribution[chunk_to_process], dest=worker)

            for worker in (range(1, size)):  # receive
                result = comm.recv(source=worker)
                results.append(result)


            final_results=""
            
            final_results = reduce_task(results,comm.Get_size())

            final_tuple = (final_results,round(time.time() - start,2))

            
            return final_tuple
        

        elif rank > 0:
            chunk_to_process = comm.recv()
            df = pd.read_csv(self.dataset_path, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], header=None)
            prefix = ['S','P']

            df['origin_with_S_P'] = df.iloc[:,2].str.startswith(tuple(prefix))

            total_s_or_p = df['origin_with_S_P'].tolist().count(True)
            result = (
            df.groupby(df.iloc[:,1]).apply(lambda x : pd.Series({
                'S_P_Percentage':  (x['origin_with_S_P'].sum() / total_s_or_p) * 100

            }))
            .reset_index()
            )
            comm.send( result.set_index(1)['S_P_Percentage'].to_dict(), dest=0)

            return ("none from slaves","NA")


            





        

        



        
     





           



        



# mpiexec -n 4 python implementation/q1/t3.py

       



        


if __name__ == '__main__':
    solution = MPISolution(dataset_path="implementation\Combined_Flights_2021.csv", dataset_size=6311871)
    answer, timetaken = solution.run()
    print(answer, timetaken)

