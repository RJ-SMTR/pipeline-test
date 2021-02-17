from prefect import task, Flow, Task, Parameter
from prefect.schedules import clocks, Schedule

diurnal   = ['rooster', 'dog']
nocturnal = ['owl', 'hampster']

# Clocks
diurnal_clock   = clocks.CronClock("51 * * * *", parameter_defaults={"animals": diurnal})
nocturnal_clock = clocks.CronClock("53 * * * *", parameter_defaults={"animals": nocturnal})

# the full schedule
schedule = Schedule(clocks=[diurnal_clock, nocturnal_clock])

@task
def wakeup(animals):
    for item in animals:
        print("Waking up animal %s" % item)

# Flow is common to both types, though with different schedules.
with Flow(name="wakuptime", schedule=schedule) as this_flow:
    animals = Parameter("animals", default=[])
    wakeup(animals)

# will run on the schedule with varying parameter values
this_flow.register("Teste")