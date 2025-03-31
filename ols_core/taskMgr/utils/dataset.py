import MNN
import pandas as pd

F = MNN.expr

class Dataset(MNN.data.Dataset):
    def __init__(self, isTrain=True, data_path="", training_files={}, shape=dict(), normalize=dict()):
        super(Dataset, self).__init__()
        self.isTrain = isTrain
        self.data_path = data_path
        self.training_files = training_files
        self.shape = shape
        self.normalize = normalize
        self.getDataset()

    def getDataset(self):
        if self.isTrain:
            train_features_path = f"{self.data_path}/" + self.training_files.get("train_features")
            data = pd.read_csv(train_features_path).to_numpy()
            if self.normalize.get("train_features"):
                data = (data - self.normalize.get("features_min")) / (self.normalize.get("features_max") - self.normalize.get("features_min"))
            self.data = data.reshape(len(data), self.shape.get("height"), self.shape.get("weight"))

            train_labels_path = f"{self.data_path}/" + self.training_files.get("train_labels")
            labels = pd.read_csv(train_labels_path).to_numpy()
            if self.normalize.get("train_labels"):
                labels = (labels - self.normalize.get("labels_min")) / (
                            self.normalize.get("labels_max") - self.normalize.get("labels_min"))
            self.labels = labels.reshape(len(labels))

        else:
            test_features_path = f"{self.data_path}/" + self.training_files.get("test_features")
            data = pd.read_csv(test_features_path).to_numpy()
            if self.normalize.get("test_features"):
                data = (data - self.normalize.get("features_min")) / (self.normalize.get("features_max") - self.normalize.get("features_min"))
            self.data = data.reshape(len(data), self.shape.get("height"), self.shape.get("weight"))

            test_labels_path = f"{self.data_path}/" + self.training_files.get("test_labels")
            labels = pd.read_csv(test_labels_path).to_numpy()
            if self.normalize.get("test_labels"):
                labels = (labels - self.normalize.get("labels_min")) / (
                            self.normalize.get("labels_max") - self.normalize.get("labels_min"))
            self.labels = labels.reshape(len(labels))

    def __getitem__(self, index):
        dv = F.const(self.data[index].flatten().tolist(), [self.shape.get("channel"), self.shape.get("height"), self.shape.get("weight")], F.data_format.NCHW)
        dl = F.const([self.labels[index]], [], F.data_format.NCHW, F.dtype.uint8)
        return [dv], [dl]

    def __len__(self):
        return len(self.data)
