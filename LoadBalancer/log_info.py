class Logger(object):
    """
    Generate Log Data for Tasks run in the Load Balancer pattern
    """

    def __init__(self):
        self.run_jobs = {}
        self.results = {}

    def _create_table_statistics(self, worker_id):
        if worker_id in self.results:
            self.results[worker_id] += 1
        else:
            self.results[worker_id] = 1

    def add(self, worker_id, job_id, result):
        self._create_table_statistics(worker_id)
        self.run_jobs[job_id] = result

    def show_processed_tasks(self):
        """Get info about run jobs and it result"""
        print("\n\n")
        if self.run_jobs:
            print("{:8} {:15}".format("Job ID", "Result"))
            for job_id, result in self.run_jobs.items():
                print("{:8} {:15}".format(job_id, result))    
        else:
            print("No jobs processed")
        
    def show_tasks_per_worker(self):
        """Get info about run tasks per worker"""
        print("\n\n")
        if self.results:
            print("{:8} {:15} {:10}".format("Key", "Worker", "# Tasks"))
            for count, (k, v) in enumerate(self.results.items(), 1):
                print("{:8} {:15} {:10}".format(count, k, v))
        else:
            print("No tasks completed")
