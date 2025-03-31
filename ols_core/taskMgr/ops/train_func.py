import MNN
import numpy as np

nn = MNN.nn
F = MNN.expr
F.lazy_eval(True)

class TrainingOperator(object):
    def __init__(self, net, train_dataloader,
                 params, data_shape, label_max,
                 loss_fun = "cross_entropy"):
        self.net = net
        self.train_dataloader = train_dataloader
        self.params = params
        self.data_shape = data_shape
        self.label_max = label_max
        self.loss_fun = f"nn.loss.{loss_fun}"

    def train_func(self, opt):
        """train function"""
        self.net.train(True)
        self.train_dataloader.reset()
        for i in range(self.train_dataloader.iter_number):
            example = self.train_dataloader.next()
            input_data = example[0]
            output_target = example[1]
            data = input_data[0]  # which input, model may have more than one inputs
            label = output_target[0]  # also, model may have more than one outputs
            predict = self.net.forward(data)
            target = F.one_hot(F.cast(label, F.int), self.label_max, 1, 0)
            loss = eval(self.loss_fun)(predict, target)
            opt.step(loss)
            if i % 100 == 0:
                print("train loss: ", loss.read())

    def test_func(self, test_dataloader):
        """test function"""
        self.net.train(False)
        test_dataloader.reset()
        correct = 0
        loss = 0
        for i in range(test_dataloader.iter_number):
            example = test_dataloader.next()
            input_data = example[0]
            output_target = example[1]
            data = input_data[0]  # which input, model may have more than one inputs
            label = output_target[0]  # also, model may have more than one outputs
            predict = self.net.forward(data)
            loss += eval(self.loss_fun)(predict, F.one_hot(F.cast(label, F.int), self.label_max, 1, 0))
            predict = F.argmax(predict, 1)
            predict = np.array(predict.read())
            label = np.array(label.read())
            correct += (np.sum(label == predict))
        test_loss = loss / test_dataloader.iter_number
        test_acc = correct * 100.0 / test_dataloader.size
        return test_acc, float(test_loss.read())

    def learning_rate_scheduler(self, lr, epoch):
        if (epoch + 1) % 2 == 0:
            lr *= 0.1
        return lr

    def getSGDOpt(self):
        opt = MNN.optim.SGD(
            self.net,
            self.params["lr"],
            self.params["momentum"],
            self.params["weight_decay"]
        )
        return opt

    def fit(self, epochs, save_path):
        epochs = int(max(epochs, 0))
        for epoch in range(epochs):
            opt = self.getSGDOpt()
            opt.learning_rate = self.learning_rate_scheduler(opt.learning_rate, epoch)
            self.train_func(opt)
        self.net.train(False)
        predict = self.net.forward(F.placeholder([1, self.data_shape.get("channel"), self.data_shape.get("height"), self.data_shape.get("weight")], F.NC4HW4))
        F.save([predict], save_path, False)




