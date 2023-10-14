import argparse
import pandas as pd
from pathlib import Path
from utils import compute_meanshift, group_by, to_csv



def main(args):
    path_to_data = Path(args.path)
    start_time = args.start_time
    window_in_hours = args.window
    stride = args.stride

    # load participant overview xlsx
    participant_overview = pd.read_excel(path_to_data/"participant-overview.xlsx", header=0, index_col=0, skiprows=1)
    # fill missing values with the mean
    participant_overview["Max heart rate"] = participant_overview["Max heart rate"].fillna(participant_overview["Max heart rate"].mean())
    # load all json files in path
    data_folders = list(path_to_data.glob("p*/fitbit"))
    data_folders.sort()
    for path_to_data in data_folders:
        fitbit = []
        print(f"Loading {path_to_data.parent.name}")
        features = ["heart_rate", "sleep", "exercise", "steps", "calories", "distance"]
        for file in [path_to_data/f"{feature}.json" for feature in features]:
            data = pd.read_json(file)
            if file.stem == "heart_rate":
                data.index = pd.to_datetime(data["dateTime"])
                data = data.drop(columns=["dateTime"])
                data = data.map(lambda x: x["bpm"])
                # sample by minute and use the exact value
                data = data.resample("1T").mean()
                # Divide heartrate in stages given the max heart rate (https://www.verywellhealth.com/heart-rate-zones-5214548)
                max_hr = participant_overview.loc[path_to_data.parent.name, "Max heart rate"]
                data = data.map(lambda x: 0 if x<0.5*max_hr else 1 if 0.5*max_hr <= x < 0.6*max_hr else 2 if 0.6*max_hr <= x < 0.7*max_hr else 3 if 0.7*max_hr <= x < 0.8*max_hr else 4 if 0.8*max_hr < x < 0.9*max_hr else 5 if 0.9*max_hr <= x < max_hr else -1)
            elif file.stem == "sleep":
                data = data["levels"]
                for i, elem in enumerate(data):
                    t = pd.DataFrame(elem["data"])
                    t["dateTime"] = t["dateTime"].apply(lambda x: pd.to_datetime(x.replace("T", " ")))
                    t = t.apply(lambda x: (x["level"], x["dateTime"], x["dateTime"]+pd.Timedelta(seconds=x["seconds"])), axis=1)
                    data[i] = t
                data = pd.concat(data.to_list())
            elif file.stem == "exercise":
                data = data.apply(lambda x: (x["activityName"], pd.to_datetime(x["startTime"]), pd.to_datetime(x["startTime"])+pd.Timedelta(milliseconds=x["duration"])), axis=1)
            elif file.stem == "steps":
                data.index = pd.to_datetime(data["dateTime"])
                data = data.drop(columns=["dateTime"])
                # steps chart (aerobics) at https://www.lyondellbasell.com/globalassets/sustainability/lifebeats/active-challenges/step-up-for-health-2017/step-count-conversion-chart.pdf
                data = data.map(lambda x: 0 if x<115 else 1 if 115 <= x < 145 else 2 if 145 <= x < 190 else 3)
            elif file.stem == "calories":
                data.index = pd.to_datetime(data["dateTime"])
                data = data.drop(columns=["dateTime"])
                # Divide data in quartiles based on data and assing a value
                q = data.quantile([0.25, 0.5, 0.75], interpolation='nearest')
                data = data.map(lambda x: 0 if x<q.iloc[0, 0] else 1 if q.iloc[0, 0] <= x < q.iloc[1, 0] else 2 if q.iloc[1, 0] <= x < q.iloc[2, 0] else 3)
            elif file.stem == "distance":
                data.index = pd.to_datetime(data["dateTime"])
                data = data.drop(columns=["dateTime"])
                # Divide data in quartiles 
                q = data.quantile([0.25, 0.5, 0.75], interpolation='nearest')
                data = data.map(lambda x: 0 if x<q.iloc[0, 0] else 1 if q.iloc[0, 0] <= x < q.iloc[1, 0] else 2 if q.iloc[1, 0] <= x < q.iloc[2, 0] else 3)
                                   
            fitbit.append(data)
        # compute segmentation
        transactions = []
        for i, feature in enumerate(fitbit):
            print(f"\tProcessing {features[i]}")
            timeseries = group_by(feature, start_time=pd.to_datetime(start_time), window_in_hours=window_in_hours, stride=stride)
            if features[i] == "sleep" or features[i] == "exercise":
                timeseries = [[(elem.iloc[j]["level"], elem.index[j], elem.iloc[j]["end"]) for j in range(len(elem))] for elem in timeseries]
            else:
                timeseries = compute_meanshift(timeseries, feature=features[i])
            to_csv(timeseries, path=f"{path_to_data}/transaction/", feature=f"{features[i]}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Fitbit data with given arguments")
    parser.add_argument("path", type=str, help="Path to the pmdata")
    parser.add_argument("--start_time", type=str, default="2019-11-01 8:00:00", help="Starting time")
    parser.add_argument("--window", type=int, default=24, help="Window in hours")
    parser.add_argument("--stride", type=int, default=24, help="Stride")

    args = parser.parse_args()
    main(args)