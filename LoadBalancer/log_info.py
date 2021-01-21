class Logger(object):
    def __init__(self):
        self.results = {}

    def add(self, worker_id):
        if worker_id in self.results:
            self.results[worker_id] += 1
        else:
            self.results[worker_id] = 1

    def show_results(self):
        # print("Beautify: %s" % self.results)
        if self.results:
            print("{:8} {:15} {:10}".format('Key','Worker','# Tasks'))
            for count, (k, v) in enumerate(self.results.items(), 1):
                print("{:8} {:15} {:10}".format(count, k, v))
        else:
            print("No tasks completed")