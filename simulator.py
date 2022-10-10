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

    def get_policy(self, policy_str):
        if policy_str == 'random':
            return self._policy_random
        elif policy_str == 'least_loaded':
            return self._policy_least_loaded
        else:
            raise ValueError(f"Unknown policy: {policy_str}")

    def add_job(self, job):
        self.jobs.append(job)

    def _policy_random(self, regions):
        # Randomly schedule a job to a region
        return random.choice(regions)

    def _policy_least_loaded(self, job, regions):
        # Schedule a job to the least loaded region
        return min(regions, key=lambda r: len(r.jobs))

    def run_scheduler(self):
        for job in self.jobs:
            if job.region is None:
                # Job is not running, schedule it
                job.set_region(self.policy_fn(self.regions))
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
