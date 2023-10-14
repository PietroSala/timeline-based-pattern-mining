from collections import defaultdict
import itertools
import os
from pathlib import Path
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.cluster import AgglomerativeClustering, MeanShift
#import Rbeast as rb
#from dtaidistance import dtw

def group_by(timeseries, start_time: datetime, window_in_hours: int, stride: int) -> pd.DataFrame:
    """
    Group a time series by a given window size.
    Parameters
    ----------
    timeseries
        Time series to group.
    start_time : datetime
        Start time of the time series.
    window_in_hours : int
        Window size in hours.
    stride : int
        Stride of the window.
    Returns
    -------
    pd.DataFrame
        Grouped time series.
    """
    if isinstance(timeseries.iloc[0], tuple):
        timeseries = pd.DataFrame([[e for e in t] for t in timeseries], columns=["level", "start", "end"])
        timeseries.index = timeseries["start"]
        timeseries = timeseries.drop(columns=["start"])
    # Compute every stride start time
    start_times = pd.date_range(start=start_time, end=timeseries.index[-1], freq=f"{stride}H")
    # Compute range of each window for each start time
    ranges = [pd.date_range(start=start_time, end=start_time + pd.Timedelta(hours=window_in_hours), periods=2) for start_time in start_times]
    # Group time series by each window
    timeseries = [timeseries.loc[np.logical_and(timeseries.index > ranges[i][0], timeseries.index <= ranges[i][1])] for i in range(len(ranges))]
    # Remove empty windows
    timeseries = [elem for elem in timeseries if len(elem) > 0]
    
    return timeseries

def compute_meanshift(timeseries:list[pd.DataFrame], feature:str) -> list[list[pd.DataFrame]]:
    """
    Compute clustering of a time series.
    Parameters
    ----------
    timeseries : pd.DataFrame
        Time series to cluster.
    method : str, optional
        Clustering method.
    Returns
    -------
    dict
        Clustering of time series.
    """
    for i, item in enumerate(timeseries):
        cluster = []
        for value in np.unique(item):
            idxs = np.where(item == value)[0]
            labels_value = MeanShift(n_jobs=-1).fit_predict(np.array([x.timestamp() for x in item.iloc[idxs].index]).reshape(-1, 1))
            for label in np.unique(labels_value):
                idxs = np.where(labels_value == label)[0]
                # Add 30s to the end of the interval
                cluster.append((f"{feature}_{value}_{label}", item.iloc[idxs[0]].name, item.iloc[idxs[-1]].name+pd.Timedelta(seconds=30)))
        timeseries[i] = sorted(cluster, key=lambda element: element[1])
    return timeseries

def compute_clustering(timeseries:list[list[pd.DataFrame]], n_clusters:int, feature_name:str, method:str = "agglomerative") -> dict:
    """
    Compute clustering of a time series.
    Parameters
    ----------
    timeseries : pd.DataFrame
        Time series to cluster.
    method : str, optional
        Clustering method.
    Returns
    -------
    dict
        Clustering of time series.
    """
    clustering = []
    transactions = np.insert(np.cumsum([len(x) for x in timeseries]), 0, 0)
    if method == "agglomerative":
        cluster = defaultdict(list)
        segments = [item.to_numpy() for sublist in timeseries for item in sublist]
        # Compute clustering and print time
        segments = dtw.distance_matrix_fast(segments)
        labels_trend = AgglomerativeClustering(n_clusters=n_clusters, metric="precomputed", linkage="average").fit_predict(segments)
    
    for i in range(len(transactions)-1):
        labels = labels_trend[transactions[i]:transactions[i+1]]
        cluster = []
        for label in np.unique(labels):
            idxs = np.where(labels == label)[0]
            cluster.extend([(f"{feature_name}_{label}", timeseries[i][idx].index[0], timeseries[i][idx].index[-1]) for idx in idxs])
        clustering.append(sorted(cluster, key=lambda element: element[1]))
        
    return clustering

def to_csv(clustering:list, path:str, feature:str) -> None:
    """
    Save clustering to csv file.
    Parameters
    ----------
    clustering : list
        Clustering to save.
    path : str
        Path of the csv file.
    Returns
    -------
    None
    """
    os.makedirs(path, exist_ok=True)
    with open(path+f"{feature}.csv", "w") as file:
        file.write("transaction,cluster,begin,end\n")
        for i, cluster in enumerate(clustering):
            # Sort by begin time and remove duplicates
            cluster = sorted(list(set(cluster)), key=lambda element: element[1])
            for elem in cluster:
                file.write(f"{i},{elem[0].lower()},{elem[1]},{elem[2]}\n")

def _compute_segmentation(timeseries: pd.DataFrame) -> list[pd.DataFrame]:
    """
    Compute segmentation of a time series.
    Parameters
    ----------
    timeseries : pd.DataFrame
        Time series to segment.
    Returns
    -------
    list[pd.DataFrame]
        Segmented time series.
    """
    beast = rb.beast(timeseries, season='none', print_progress=False, print_options=False)
    trend = beast.trend.cp
    trend = trend[~np.isnan(trend)]
    trend = np.sort(trend)
    trend = np.append(trend, len(timeseries)-1)
    trend = np.insert(trend, 0, 0)
    trend = trend.astype(int)
    return [timeseries[trend[i]:trend[i+1]] for i in range(len(trend)-1)]

def compute_segmentation(timeseries:list[pd.DataFrame], steps:int = 1) -> list[list[pd.DataFrame]]:
    """
    Compute segmentation of a time series.
    Parameters
    ----------
    timeseries : np.ndarray
        Time series to segment.
    steps : int, optional
        Number of recursive segmentations on timeseries.
    Returns
    -------
    list[list[pd.DataFrame]]
        Segmented time series.
    """
    timeseries = [_compute_segmentation(elem) if len(elem)>10 else [elem] for elem in timeseries]
    for _ in range(1, steps):
        timeseries = [list(itertools.chain.from_iterable([_compute_segmentation(subelem) if len(subelem)>10 else [subelem] for subelem in elem])) for elem in timeseries]

    return timeseries

def filter(path:str, cluster_labels: set, confidence:float):
    """
    Filter and store Fitbit data files based on specified cluster labels and confidence level.

    Parameters:
    - path (str): The path to the directory containing Fitbit data files.
    - cluster_labels (set): A set of cluster labels to filter by.
    - confidence (float): A confidence level used to determine the proportion of data to keep.

    Returns:
    - proportions (dict): A dictionary mapping participant names to the proportions of retained data.

    The function processes Fitbit data files located in subdirectories of 'path'. It iterates through
    the data files and filters records with cluster labels in 'cluster_labels'. Filtered data is
    saved to new CSV files in a 'filtered' subdirectory. The 'confidence' parameter specifies the
    confidence level for retaining data.

    The function returns a dictionary that maps participant names to the proportions of data retained
    based on the filtering criteria. Proportions are calculated as (total data * confidence) / filtered data.

    Example usage:
    proportions = filter("/path/to/fitbit/data", set(["rem", "run"]), 0.05)
    """
    FILE_LIST = sorted(list(Path(path).glob("p*/fitbit/transaction/")))
    observations = []
    proportions = defaultdict(float)
    for file_path in FILE_LIST:
        filtered = 0
        total = 0
        for file in file_path.glob("*.csv"):
            data = pd.read_csv(file)
            test = data.loc[data["cluster"].isin(cluster_labels)]
            if not test.empty:
                observations.append((test, len(data)))
                path = file_path.parent.parent/"filtered/"
                os.makedirs(path, exist_ok=True)
                test.to_csv(path/f"{file.stem}.csv", index=False)
                filtered += len(test)
                total += len(data)
        proportions[file_path.parent.parent.name] = total*confidence/filtered

    return proportions
