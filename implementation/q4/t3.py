"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""

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

        raise NotImplementedError("Implement your logic here")

if __name__ == '__main__':
    solution = MPISolution(dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
    answer, timetaken = solution.run()
    print(answer, timetaken)

