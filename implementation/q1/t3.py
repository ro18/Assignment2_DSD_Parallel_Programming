"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""

import time
import pandas as pd
from mpi4py import MPI

class MPISolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, dataset_path=None, dataset_size=None):
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self):
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()

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
            start = time.time()
            for worker in range(1, size):
                chunk_to_process = worker-1
                comm.send(chunk_distribution[chunk_to_process], dest=worker)

        # receive and aggregate results from slave
            results = []
            for worker in (range(1, size)):  # receive
                result = comm.recv(source=worker)
                results.append(result)
                print(f'received from Worker slave {worker}:{result}')

            out = sum(results)
            return out, round(time.time() - start,2)
        # out = 0
        # for r in results:
        #     out = out + r.isna().sum()

        # print(f'missing values: {out}', '\ndone.')


        elif rank > 0:
            chunk_to_process = comm.recv()
            df = pd.read_csv(self.dataset_path, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], header=None)
            prefix = ['S','P']
            matched_rows=df.iloc[:,2].str.startswith(tuple(prefix))
            result=matched_rows.tolist().count(True)
            comm.send(result, dest=0)


if __name__ == '__main__':
    solution = MPISolution(dataset_path="C:\\Users\\R_CORDEI\\Desktop\\DSD-A2\\Assignment2_DSD_Parallel_Programming\\implementation\\Combined_Flights_2021.csv", dataset_size=6311871)
    answer, timetaken = solution.run()
    print(answer, timetaken)

