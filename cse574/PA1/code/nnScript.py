import numpy as np
from scipy.optimize import minimize
import scipy
from scipy.io import loadmat
from math import sqrt
import pickle

def initializeWeights(n_in,n_out):
    epsilon = sqrt(6) / sqrt(n_in + n_out + 1);
    W = (np.random.rand(n_out, n_in + 1)*2* epsilon) - epsilon;
    return W

def sigmoid(z):
    return 1.0 / (1.0 + np.exp(-1.0 * z));

def preprocess():
    mat_contents  = scipy.io.loadmat('mnist_all.mat')
    
    train_list = []
    validation_list =[]
    test_list = []

    label_train = []
    label_validation = []
    label_test = []

    for k in range(0,10):
        train = "train" + str(k)
        test = "test" + str(k)

        trainlen = len(mat_contents[train]) - 1000
        validlen = len(mat_contents[train])

        for i in range(0,trainlen):
            insertval = mat_contents[train][i]
            train_list.append(insertval)
            label_train.append(k)

        for m in range(trainlen,validlen):
            insertval = mat_contents[train][m]
            validation_list.append(insertval)
            label_validation.append(k)

        for j in range(0,len(mat_contents[test])):
            insertval = mat_contents[train][j]
            test_list.append(insertval)
            label_test.append(k)

    traind_temp= np.array(train_list)
    validd_temp = np.array(test_list)
    testd_temp= np.array(validation_list)

    train_label = np.array(label_train)
    validation_label = np.array(label_validation)
    test_label = np.array(label_test)

    # feature selection
    temp_td = traind_temp.astype(bool)
    temp_td_c = np.any(temp_td, axis = 0)
    temp_atd=[]
    temp_avd=[]
    temp_ated=[] 
    for i in range(0,len(temp_td_c)):
        if (temp_td_c[i]==True):
            temp_atd.append(traind_temp[:,i])
            temp_avd.append(validd_temp[:,i])
            temp_ated.append(testd_temp[:,i])
    train_data  = (np.array(temp_atd).T)/255
    validation_data  = (np.array(temp_avd).T)/255
    test_data  = (np.array(temp_ated).T)/255

    return train_data, train_label, validation_data, validation_label, test_data, test_label


def nnObjFunction(params, *args):

    n_input, n_hidden, n_class, training_data, training_label, lambdaval = args
    w1 = params[0:n_hidden * (n_input + 1)].reshape((n_hidden, (n_input + 1)))
    w2 = params[(n_hidden * (n_input + 1)):].reshape((n_class, (n_hidden + 1)))
                                                            # w1.shape = (50L, 785L)
                                                            # w2.shape = (10L, 51L)
    ## Adding Bias input node
    training_data = np.concatenate((training_data,np.ones([training_data.shape[0],1])),axis = 1)
                                                            # training_data.shape = (50000L, 785L)
    ## Extracting True Label
    Y = np.zeros((training_label.shape[0],10));
    for y in range(training_label.shape[0]):
        Y[y][training_label[y]] = 1                                 # Ylabel.shape = (50000L, 10L)
    ## calcuate Zj , Ol
    aj = np.dot(training_data,w1.T)                                 # aj.shape = (50000L, 50L)
    zj50 = sigmoid(aj)                                              # zj50.shape = (50000L, 50L)
    ## Adding z(m+1) bias node value 1 to zj
    zj51 = np.concatenate((zj50,np.ones([zj50.shape[0],1])),axis =1)
    bl = np.dot(zj51,w2.T)                                          # bl.shape = (50000L, 51L)
    ol = sigmoid(bl)                                                # ol.shape = (50000L, 10L)

    ## Back Propogation
    ## delta and Error

    delta = (Y - ol) * (1 - ol) * (ol)                              # delta.shape  =(50000L, 10L)
    Error = np.sum(np.power(Y-ol,2))/2

    ## derivatives of w1 and W2
    grad_w2 = (-1) * (np.dot(delta.T,zj51))                         # grad_w2.shape = (10L, 51L)
    grad_w1 = np.dot(training_data.T,((np.dot(delta,w2)) * (zj51 * (zj51 - 1)))) # Equation No. 12
    grad_w1 = grad_w1[:,:-1].T                                      # grad_w1.shape = (50L, 785L)

    ## Regularization
    grad_w1 = (grad_w1+(lambdaval * w1))/(training_data.shape[0])
    grad_w2 = (grad_w2+(lambdaval * w2))/(training_data.shape[0])

    w1_sum_w2 = np.sum(np.square(w1)) + np.sum(np.square(w2))
    lambda_sum = (lambdaval/(2 * training_data.shape[0])) * w1_sum_w2

    obj_val = (Error/(training_data.shape[0]))  +  lambda_sum
    obj_grad = np.concatenate((grad_w1.flatten(), grad_w2.flatten()),0)

    print (obj_val)

    return (obj_val,obj_grad)


def nnPredict(w1,w2,data):

    labels = np.empty((0,1))
    data = np.concatenate((data,np.ones([data.shape[0],1])),axis =1)
    aj = np.dot(data,w1.T)                                          # aj.shape = (50000L, 50L)
    zj50 = sigmoid(aj)                                              # zj50.shape = (50000L, 50L)
    zj51 = np.concatenate((zj50,np.ones([zj50.shape[0],1])),axis =1)
    bl = np.dot(zj51,w2.T)                                          # bl.shape = (50000L, 51L)
    ol = sigmoid(bl)                                                # ol.shape = (50000L, 10L)

    for i in range(data.shape[0]):
        labels=np.append(labels,np.argmax(ol[i]))

    return labels

"""**************Neural Network Script Starts here********************************"""

train_data, train_label, validation_data,validation_label, test_data, test_label = preprocess();

n_input = train_data.shape[1];
n_hidden = 50;
n_class = 10;
lambdaval = 0.1;

initial_w1 = initializeWeights(n_input, n_hidden);
initial_w2 = initializeWeights(n_hidden, n_class);

initialWeights = np.concatenate((initial_w1.flatten(), initial_w2.flatten()),0)
args = (n_input, n_hidden, n_class, train_data, train_label, lambdaval)

opts = {'maxiter' : 50}    # Preferred value.
nn_params = minimize(nnObjFunction, initialWeights, jac=True, args=args,method='CG', options=opts)

w1 = nn_params.x[0:n_hidden * (n_input + 1)].reshape( (n_hidden, (n_input + 1)))
w2 = nn_params.x[(n_hidden * (n_input + 1)):].reshape((n_class, (n_hidden + 1)))

predicted_label = nnPredict(w1,w2,train_data)
print('\n Training set Accuracy:' + str(100*np.mean((predicted_label == train_label).astype(float))) + '%')

predicted_label = nnPredict(w1,w2,validation_data)
print('\n Validation set Accuracy:' + str(100*np.mean((predicted_label == validation_label).astype(float))) + '%')

predicted_label = nnPredict(w1,w2,test_data)
print('\n Test set Accuracy:' + str(100*np.mean((predicted_label == test_label).astype(float))) + '%')

pickleFile = open("params.pickle", 'wb')
pickle.dump([w1, w2, n_hidden, lambdaval], pickleFile)
