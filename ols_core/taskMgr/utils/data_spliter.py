import logging

def getSplitDataIndex(dataLens, phones_num):
    startINDs = []
    endINDs = []
    samples = dataLens // phones_num
    for i in range(phones_num):
        start_indx = samples * i
        startINDs.append(start_indx)
        if i == phones_num - 1:
            end_idx = dataLens
        else:
            end_idx = samples * (i+1)
        endINDs.append(end_idx)
    return startINDs, endINDs

def getSplitDataset(dataset, startINDs, endINDs, split_index):
    data = dataset.data
    labels = dataset.labels
    dataset.data = data[startINDs[split_index]:endINDs[split_index]]
    dataset.labels = labels[startINDs[split_index]:endINDs[split_index]]
    return dataset

def splitDataset(dataset, phones_num, split_index):
    startINDs, endINDs = getSplitDataIndex(len(dataset), phones_num)
    dataset = getSplitDataset(dataset, startINDs, endINDs, split_index)
    print(f"train_startINDs[split_index] = {startINDs[split_index]}, train_endINDs[split_index] = {endINDs[split_index]}")
    return dataset