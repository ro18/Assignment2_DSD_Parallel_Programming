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

                    # print(f"items:{out.items()}")

                    # print("key dict")
                    # print(f"key:{key}")


                    if key in reduce_out:
                        if not math.isnan(value):
                            reduce_out[key] = reduce_out.get(key) + value
                    else:
                        reduce_out[key] = value

                    # print("output now")
                    # print(reduce_out)
            

               
           


            #print("max")

            #print(max(reduce_out.values()))

            for key, value in reduce_out.items():
                print(f"value:{value}")
         
                reduce_out[key] = value/self.process_size

            #print("max key")


            #print(max(reduce_out, key=reduce_out.get))


            return max(reduce_out, key=reduce_out.get)
        
        start = time.time()
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()

        results = []

        if rank == 0:
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

            slave_workers = size - 1
            chunk_distribution = distribute_rows(n_rows=self.dataset_size, n_processes=slave_workers)

            # distribute tasks to slaves
            
            for worker in range(1, size):
                chunk_to_process = worker-1
                comm.send(chunk_distribution[chunk_to_process], dest=worker)

                 # receive and aggregate results from slave

            for worker in (range(1, size)):  # receive
                result = comm.recv(source=worker)
                results.append(result)
                #print(f'received from Worker slave {worker}:{result}')



            final_results=""
            
            final_results = reduce_task(results,comm.Get_size())

            #print("here")


            final_tuple = (final_results,round(time.time() - start,2))

            
            return final_tuple
        



            


            


   






        elif rank > 0:
            chunk_to_process = comm.recv()
            df = pd.read_csv(self.dataset_path, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], header=None)
            prefix = ['S','P']

            df['origin_with_S_P'] = df.iloc[:,2].str.startswith(tuple(prefix))

            #print(df['origin_with_S_P'])

            total_s_or_p = df['origin_with_S_P'].tolist().count(True)

            #print("total")

           # print(total_s_or_p)

            result = (
            df.groupby(df.iloc[:,1]).apply(lambda x : pd.Series({
                'S_P_Percentage':  (x['origin_with_S_P'].sum() / total_s_or_p) * 100
                #'S_P_Percentage':  x['origin_with_S_P'].sum()

            }))
            .reset_index()
            )
            #print(result.set_index(1)['S_P_Percentage'].to_dict())
            comm.send( result.set_index(1)['S_P_Percentage'].to_dict(), dest=0)

            return ("none from slaves","NA")


            





        

        



        
     





           



        



# mpiexec -n 4 python implementation/q1/t3.py

       



        


if __name__ == '__main__':
    solution = MPISolution(dataset_path="implementation\Combined_Flights_2021.csv", dataset_size=6311871)
    answer, timetaken = solution.run()
    print(answer, timetaken)

