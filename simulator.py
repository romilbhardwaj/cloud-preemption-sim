# Cloud preemption simulator
# Simulates preemption of cloud instances in different regions
from numpy import random

global CLOCK


class Job:
    # Job runs infinitely
    def __init__(self, name, region):
        self.name = name
        self.region = region
        self.region_running_time = 0
        self.total_running_time = 0
        self.history = []
        self.num_preemptions = 0

    def __str__(self):
        return f"{self.name}"

    def preempt(self):
        self.region = None
        self.region_running_time = 0
        self.num_preemptions += 1

    def set_region(self, region):
        self.region = region

    def tick(self):
        if self.region is not None:
            self.region_running_time += 1
            self.total_running_time += 1
            self.history.append((CLOCK, str(self.region)))


class Region:
    def __init__(self, name, p):
        # p is preemption probability for a job in this region at every tick
        self.name = name
        self.jobs = []
        # TODO: Preemption probability could also be a function of the number of jobs in the region
        self.p = p
        self.history = []

    def preempt(self, job):
        # print(f"Preempted {job}")
        self.jobs.remove(job)
        job.preempt()

    def add_job(self, job):
        self.jobs.append(job)

    def tick(self):
        for job in self.jobs:
            if random.random() < self.p:
                self.preempt(job)
                self.history.append((CLOCK, [str(j) for j in self.jobs]))

    def __str__(self):
        return f"{self.name}"


class Scheduler:
    def __init__(self, regions, policy='random'):
        self.regions = regions
        self.jobs = []
        self.policy_fn = self.get_policy(policy)

    def get_policy(self, policy_str) -> callable:
        if policy_str == 'random':
            return self._policy_random
        elif policy_str == 'least_loaded':
            return self._policy_least_loaded
        elif policy_str == 'epsilon_greedy':
            return self._policy_epsilon_greedy
        else:
            raise ValueError(f"Unknown policy: {policy_str}")

    def add_job(self, job):
        self.jobs.append(job)

    def _policy_random(self) -> Region:
        # Randomly schedule a job to a region
        return random.choice(self.regions)

    def _policy_epsilon_greedy(self) -> Region:
        # Schedule a job to the region that has been the most successful, with probability 1-epsilon to schedule to a random region
        # TODO - Change this hardcoded value to be a policy parameter
        epsilon = 0.3
        best_region = None
        longest_running_time = 1
        # Apply epsilon-greedy exploration
        if random.random() < epsilon:
            best_region = self._policy_random()
            print("Randomly exploring a new region: ", best_region)
            return best_region
        else:
            for job in self.jobs:
                if job.region_running_time > longest_running_time:
                    best_region = job.region
                    longest_running_time = job.region_running_time
            if best_region is None:
                # If no jobs have been running, randomly choose a region
                best_region = self._policy_random()
            print("Most successful job and region: ", job, best_region)
            return best_region

    def _policy_least_loaded(self) -> Region:
        # Schedule a job to the least loaded region
        return min(self.regions, key=lambda r: len(r.jobs))

    def run_scheduler(self):
        for job in self.jobs:
            if job.region is None:
                # Job is not running, schedule it
                job.set_region(self.policy_fn())
                job.region.add_job(job)
                global CLOCK
                print(f"t={CLOCK}: Scheduled {job} to {job.region}")

    def tick(self):
        self.run_scheduler()


class Environment:
    def __init__(self, regions, sched_policy='random'):
        self.clock = 0
        self.regions = regions
        self.jobs = []
        self.scheduler = Scheduler(self.regions, policy=sched_policy)

    def add_job(self, job):
        self.jobs.append(job)
        self.scheduler.add_job(job)

    def tick(self):
        self.clock += 1
        global CLOCK
        CLOCK = self.clock
        self.scheduler.tick()
        for job in self.jobs:
            job.tick()
        for region in self.regions:
            region.tick()


if __name__ == '__main__':
    r1 = Region("r1", 0.1)
    r2 = Region("r2", 0.1)
    regions = [r1, r2]
    env = Environment(regions)
    for i in range(10):
        env.add_job(Job(f"job{i}", None))
    for i in range(100):
        env.tick()
