TODO:
1) currently only ran for cleveland. need to run for san francisco, new orleans, generate dataset for miami and run for miami
2) add a benchmark for random bus-dependent area to any grocery store
3) weight the benchmarks so getting to hospital, grocery store is more important than starbucks/mcdonalds
4) add a few more possible values for each of the params in the Q table (ie maybe look for stops within 400 meters or within 3 miles)
5) increase the epochs
6) finish paper

overall approach:
use Q learning to figure out how to edit the existing bus plan to be more equitable

benchmarks:
1) random bus-dependent area to any hospital
2) random bus-dependent area to any park
3) random bus-dependent area to random starbucks
4) random bus-dependent area to random mcdonalds

Total bus minutes:
the total amount of time bus drivers need per day to complete all of the routes
i.e. if a route takes 2 hours to complete, and runs once per hour (24x per day), this route requires 2*24 = 48 total hours to complete each day
Goal is to keep this within 10% of the original. i.e. it's kinda cheating if we just increase the frequencies for all the routes without any tradeoffs

Actions:
add a stop to a route
remove a stop from a route
replace a stop on a route
make a route run more frequently
make a route run less frequently
(if the total bus minutes is more than 10% higher than the original, only actions that require less work are allowed: decreasing freq, removing stops)

Q-Table
each action has an associated parameter X.
when adding/replacing stops, we choose a random stop within X meters of the original
when increasing/decreasing frequencies, we modify the frequency by X mins
the Q table keeps track of, for each action, which valaue for X, on average, resulted in the best overall commute time
right now the options are +/- 5, 10, 20 mins for freqs and within 800, 1600, 3200 meters for adding stops. these should probably be changed

In the final visualization: 
first graph = how the overall commute time changed
second graph = how the total bus minutes changed
third graph = bus route plan comparison between the original and the improed

blue = unchanged routes
orange = routes with higher frequency in the improved version
purple = routes with higher frequency in the original version
yellow = stops/routes only in the improved version
red = stops/routes only in the original version

