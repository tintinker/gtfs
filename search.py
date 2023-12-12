from pathlib import Path
import random
from lib.route_plan_dataset import RoutePlanDataset
import lib.util as util
import matplotlib.pyplot as plt
import logging
from tqdm import tqdm
import copy
from lib.optimize.benchmarks import Benchmarks
import lib.optimize.random_actions as actions

logging.basicConfig(level=logging.DEBUG)


SAVE_FOLDER = Path("improved_plans")
SAVE_FOLDER.mkdir(parents=True, exist_ok=True)

DATASETS = [
    RoutePlanDataset.load("datasets/cleveland"),
    RoutePlanDataset.load("datasets/atlanta"),
    RoutePlanDataset.load("datasets/miami"),
    RoutePlanDataset.load("datasets/new_orleans"),
    RoutePlanDataset.load("datasets/washington_dc"),
    RoutePlanDataset.load("datasets/sanfrancisco")

]

INITIAL_EXPLORE_PROBABILITY = 0.7
MIN_EXPLORE_PROBABILITY = 0.2
TOLERANCE_PROBABILITY = 0.2
EPOCHS = 200

def create_logger(dataset):
    name = dataset.name
    current_save_folder = Path(SAVE_FOLDER / name)
    current_save_folder.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"{name}")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(current_save_folder / f"{name}.log")
    logger.addHandler(handler)
    return logger

def search(dataset, logger):
    name = dataset.name

    current_save_folder = Path(SAVE_FOLDER / name)
    current_save_folder.mkdir(parents=True, exist_ok=True)


    stops = dataset.node_attributes
    bus_dependent = (stops.rentor_occupied_units_percent > 0.4) & (stops.vehicles_percent < 0.5) & (stops.income_to_poverty_under_200_percent > 0.2)
    stops["color"] = bus_dependent.apply(lambda row: "red" if row else "blue")
    util.show_viz(stops, show=False, save_filename=current_save_folder / f"{name}_busdependent.png", markersize=3, title="Bus Dependent Stops", xlabel="Longitude", ylabel="Latitude")
        

    original_bps = dataset.get_original_bus_plan_state()

    viz = util.visualize_bps(original_bps, dataset.node_attributes)
    util.show_viz(viz, show=False, save_filename=current_save_folder / f"{name}_original.png", title="Original Bus Plan (shortest interval in mins)", xlabel="Longitude", ylabel="Latitude")



    benchmarker = Benchmarks(dataset, logger)
    original_score, _ = benchmarker.all_benchmarks(original_bps)
    original_bus_minutes = original_bps.total_bus_minutes
    original_bps.name = f"{name}_original"
    original_bps.save_folder = current_save_folder

    improved_bps = copy.deepcopy(original_bps)
    improved_bps.name = f"{name}_improved"
    improved_bps.save_folder = current_save_folder

    best_score = original_score
    best_score_bus_mins = original_bus_minutes
    logger.info(f"Current best: {best_score}")
    logger.info(f"original valid: {original_bps.valid}")
    logger.info(f"original total time: {best_score_bus_mins}")

    Q_params = {
            actions.increase_random_route_frequency.__name__: {5: 0, 10: 0, 20: 0},
            actions.decrease_random_route_frequency.__name__: {5: 0, 10: 0, 20: 0},
            actions.add_random_stop.__name__: {800: 0, 1600: 0, 3200: 0},
            actions.remove_random_stop.__name__: {0: 0},
            actions.replace_random_stop.__name__: {800: 0, 1600: 0, 3200: 0}
    }

    Q_update_counts = {
            actions.increase_random_route_frequency.__name__: {5: 1, 10: 1, 20: 1},
            actions.decrease_random_route_frequency.__name__: {5: 1, 10: 1, 20: 1},
            actions.add_random_stop.__name__: {800: 1, 1600: 1, 3200: 1},
            actions.remove_random_stop.__name__: {0: 1},
            actions.replace_random_stop.__name__: {800: 1, 1600: 1, 3200: 1}
    }

    def choose_optimal_action(action_list):
        return min(action_list, key = lambda a: min(Q_params[a.__name__].values()))
    def get_optimal_param(action):
        return min(Q_params[action.__name__], key = lambda p: Q_params[action.__name__][p])



    scores = []
    bus_minutes = []
    for i in tqdm(range(EPOCHS), leave=False):
        current_bus_minutes = improved_bps.total_bus_minutes
        bus_minutes.append(current_bus_minutes)

        if current_bus_minutes > original_bus_minutes * 1.1:
            possible_actions = actions.decreased_bus_mins_actions + actions.neutral_actions
        else:
            possible_actions = actions.increased_bus_mins_actions + actions.decreased_bus_mins_actions + actions.neutral_actions

        if random.random() < max(MIN_EXPLORE_PROBABILITY, INITIAL_EXPLORE_PROBABILITY * (0.99 ** i)):
            action = random.choice(possible_actions)
            params = random.choice(list(Q_params[action.__name__].keys()))
        else:
            action = choose_optimal_action(possible_actions)
            params = get_optimal_param(action)

        description, undo = action(improved_bps, params)

        current_score, _ = benchmarker.all_benchmarks(improved_bps)
        scores.append(current_score)


        logging.info(str(current_score))
        logging.info(Q_params)

        additional_tolerance = best_score * .1 if random.random() < TOLERANCE_PROBABILITY else 0

        n = Q_update_counts[action.__name__][params] + 1
        Q_params[action.__name__][params] = (1/n) * (current_score - best_score) + (1 - 1/n) *  Q_params[action.__name__][params]
        Q_update_counts[action.__name__][params] = n

        if current_score < best_score + additional_tolerance:
            best_score = current_score
            best_score_bus_mins = improved_bps.total_bus_minutes
            logger.info(description)
            logger.info(f"Current best: {best_score}")
            
            logger.info(f"improved valid: {improved_bps.valid}")

            logger.info(f"improved total time: {improved_bps.total_bus_minutes}")
            
            improved_bps.save()
        else:
            undo(improved_bps)

    logger.info(",".join([str(s) for s in scores]))
    logger.info(",".join([str(bm) for bm in bus_minutes]))
    plt.clf()
    plt.plot(scores)
    plt.title('Scores')
    plt.axhline(y=original_score, color='red', linestyle='--', label='original score')
    plt.axhline(y=best_score, color='green', linestyle='--', label='best score')
    plt.xlabel('Epoch')
    plt.ylabel('Score')
    plt.legend()
    plt.savefig(current_save_folder / f"{name}_scores.png")

    plt.clf()
    plt.plot(bus_minutes)
    plt.title('Bus Minutes')
    plt.axhline(y=original_bus_minutes, color='red', linestyle='--', label='original bus minutes')
    plt.axhline(y=best_score_bus_mins, color='green', linestyle='--', label='best score bus minutes')
    plt.xlabel('Epoch')
    plt.ylabel('Bus Minutes')
    plt.legend()
    plt.savefig(current_save_folder / f"{name}_busminutes.png")

    logger.info(f"Improvement: {current_score - original_score}")

    improved_bps.save()

    viz = util.visualize_bps_diff(original_bps, improved_bps, dataset.node_attributes)
    util.show_viz(viz, show=False, save_filename = current_save_folder / f"{name}_diff.png", title="Bus Plan Comparison", xlabel="Longitude", ylabel="Latitude")

if __name__ == '__main__':
    for dataset in tqdm(DATASETS):
        logger = create_logger(dataset)

        for tries in range(5):
            try:
                search(dataset, logger)
                break
            
            except Exception as e:
                logger.warn(f"Exception ocurred. Restarting if avilable. Try {tries} of 5")
                logger.warn(e)
