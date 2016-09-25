import numpy as np
from scipy.io import loadmat
from scipy.optimize import minimize
from sklearn.svm import LinearSVC
from sklearn.svm import SVC
import pickle

def preprocess():
    mat = loadmat('mnist_all.mat')  # loads the MAT object as a Dictionary

    n_feature = mat.get("train1").shape[1]
    n_sample = 0
    for i in range(10):
        n_sample = n_sample + mat.get("train" + str(i)).shape[0]
    n_validation = 1000
    n_train = n_sample - 10 * n_validation

    # Construct validation data
    validation_data = np.zeros((10 * n_validation, n_feature))
    for i in range(10):
        validation_data[i * n_validation:(i + 1) * n_validation, :] = mat.get("train" + str(i))[0:n_validation, :]

    # Construct validation label
    validation_label = np.ones((10 * n_validation, 1))
    for i in range(10):
        validation_label[i * n_validation:(i + 1) * n_validation, :] = i * np.ones((n_validation, 1))

    # Construct training data and label
    train_data = np.zeros((n_train, n_feature))
    train_label = np.zeros((n_train, 1))
    temp = 0
    for i in range(10):
        size_i = mat.get("train" + str(i)).shape[0]
        train_data[temp:temp + size_i - n_validation, :] = mat.get("train" + str(i))[n_validation:size_i, :]
        train_label[temp:temp + size_i - n_validation, :] = i * np.ones((size_i - n_validation, 1))
        temp = temp + size_i - n_validation

    # Construct test data and label
    n_test = 0
    for i in range(10):
        n_test = n_test + mat.get("test" + str(i)).shape[0]
    test_data = np.zeros((n_test, n_feature))
    test_label = np.zeros((n_test, 1))
    temp = 0
    for i in range(10):
        size_i = mat.get("test" + str(i)).shape[0]
        test_data[temp:temp + size_i, :] = mat.get("test" + str(i))
        test_label[temp:temp + size_i, :] = i * np.ones((size_i, 1))
        temp = temp + size_i

    # Delete features which don't provide any useful information for classifiers
    sigma = np.std(train_data, axis=0)
    index = np.array([])
    for i in range(n_feature):
        if (sigma[i] > 0.001):
            index = np.append(index, [i])
    train_data = train_data[:, index.astype(int)]
    validation_data = validation_data[:, index.astype(int)]
    test_data = test_data[:, index.astype(int)]

    # Scale data to 0 and 1
    train_data /= 255.0
    validation_data /= 255.0
    test_data /= 255.0

    return train_data, train_label, validation_data, validation_label, test_data, test_label


def sigmoid(z):
    return 1.0 / (1.0 + np.exp(-z))

def blrObjFunction(initialWeights, *args):
    train_data, labeli = args
    n_data = train_data.shape[0]
    n_features = train_data.shape[1]

    # Adding bias to train_data
    train_data1 = np.concatenate((np.ones([int(train_data.shape[0]),1]),train_data), axis = 1)

    # calculation of thetha_n = sigmoid(w.T * X)
    temp = (np.dot(initialWeights, train_data1.T)).T
    thetha_n = sigmoid(temp)

    #Calculating the error function
    log_thethan = np.log(thetha_n).reshape([int(thetha_n.shape[0]), 1])
    log_1_thethan = np.log(1-thetha_n).reshape([int(thetha_n.shape[0]), 1])

    error_term1 = np.dot(labeli.T, log_thethan)
    error_term2 = np.dot((1-labeli).T, log_1_thethan)
    error_sum = (error_term1 + error_term2)/float(n_data)
    error = (-1) * (error_sum[0])

    diff = (thetha_n.reshape(labeli.shape) - labeli).T
    term = np.dot(diff,train_data1)
    term = term/float(n_data)
    error_grad = term.flatten()

    return error, error_grad


def blrPredict(W, data):
    data1 = np.concatenate((np.ones([int(data.shape[0]),1]),data), axis = 1)
    label = np.zeros((data.shape[0], 1))
    temp = np.dot(data1, W)
    temp1 = sigmoid(temp)
    for i in range(label.shape[0]):
        label[i] = np.argmax(temp1[i])

    return label


def mlrObjFunction(params, *args):
    train_data1, labeli = args
    train_data = np.concatenate((np.ones([int(train_data1.shape[0]), 1]),train_data1), axis=1)
    n_data = train_data.shape[0]
    weights = params.reshape([716, 10])
    error = 0

    # Calculating the exp values respective to each class
    exp_list = []
    # Calculating individual W.T * X for each class
    # output is 10 numpy arrays each of size of 50000 * 1
    for i in range(0, 10):
        exp_list.append(np.exp(np.dot(train_data, weights.T[i])))

    exp_sum = np.zeros(exp_list[0].shape)
    exp_prob = np.zeros([n_data, 10])

    # Calculating sum of 10 arrays W.T * X  for each class
    # Output is 50000 * 1
    for i in range(0, 10):
        exp_sum = np.add(exp_sum, exp_list[i])

    # calculating probabilities for each class
    # Output is 50000 * 10 i.e. 10 probabilities for each input
    # exp_prob contains all probabilites for each output class
    for i in range(0, 10):
        exp_prob[::, i] = np.divide(exp_list[i], exp_sum)

    # Equation 5 complete - Likelihood calculations:
    log_theta = -1 * np.log(exp_prob)

    for n in range(0, n_data):
        for k in range(labeli.shape[1]):
            error += (labeli[n][k] * log_theta[n][k])
    error = (error / float(n_data))

    # Equation 7 complete
    theta_minus_labeli = exp_prob - labeli
    error_grad = (np.dot(theta_minus_labeli.T, train_data)) / float(n_data)

    # Equation 8 complete
    error_grad = error_grad.T.flatten()
    return error, error_grad

def mlrPredict(W, data):
    data1 = np.concatenate(((np.ones([int(data.shape[0]), 1])), data), axis=1)
    label = np.zeros((data.shape[0], 1))
    temp = np.dot(data1, W)
    temp1 = sigmoid(temp)
    for i in range(label.shape[0]):
        label[i] = np.argmax(temp1[i])

    return label

"""
Script for Logistic Regression
"""
train_data, train_label, validation_data, validation_label, test_data, test_label = preprocess()

n_class = 10
n_train = train_data.shape[0]
n_feature = train_data.shape[1]
Y = np.zeros((n_train, n_class))
for i in range(n_class):
    Y[:, i] = (train_label == i).astype(int).ravel()

# Logistic Regression with Gradient Descent
W = np.zeros((n_feature + 1, n_class))
initialWeights = np.zeros((n_feature + 1, 1))
opts = {'maxiter': 100}
for i in range(n_class):
    labeli = Y[:, i].reshape(n_train, 1)
    args = (train_data, labeli)
    nn_params = minimize(blrObjFunction, initialWeights, jac=True, args=args, method='CG', options=opts)
    W[:, i] = nn_params.x.reshape((n_feature + 1,))

# Find the accuracy on Training Dataset
predicted_label = blrPredict(W, train_data)
print('\n Training set Accuracy:' + str(100 * np.mean((predicted_label == train_label).astype(float))) + '%')

# Find the accuracy on Validation Dataset
predicted_label = blrPredict(W, validation_data)
print('\n Validation set Accuracy:' + str(100 * np.mean((predicted_label == validation_label).astype(float))) + '%')

# Find the accuracy on Testing Dataset
predicted_label = blrPredict(W, test_data)
print('\n Testing set Accuracy:' + str(100 * np.mean((predicted_label == test_label).astype(float))) + '%')

"""
Script for Support Vector Machine
"""
print('\n\n--------------SVM-------------------\n\n')
##################
# YOUR CODE HERE #
##################
y_train = np.asarray(train_label).ravel()
y_train1 = np.asarray(validation_label).ravel()
y_train2 = np.asarray(test_label).ravel()

#Linear kernel
print('Linear kernel')
LS = LinearSVC()
LS.fit(train_data, y_train)

# Find the accuracy on Training Dataset
y_pred = LS.predict(train_data)
print('\n Training set Accuracy:' + str(100 * np.mean((y_pred == y_train).astype(float))) + '%')

# Find the accuracy on Validation Dataset
y_pred1 = LS.predict(validation_data)
print('\n Validation set Accuracy:' + str(100 * np.mean((y_pred1 == y_train1).astype(float))) + '%')

# Find the accuracy on Testing Dataset
y_pred2 = LS.predict(test_data)
print('\n Test set Accuracy:' + str(100 * np.mean((y_pred2 == y_train2).astype(float))) + '%')

#-------------------------------------------------------------------------------------------------------

#Radial basis Function with gamma=1
print('Radial basis Function with gamma=1')
LS_R = SVC(gamma=1)
LS_R.fit(train_data, y_train)

# Find the accuracy on Training Dataset
y_pred = LS_R.predict(train_data)
print('\n Training set Accuracy:' + str(100 * np.mean((y_pred == y_train).astype(float))) + '%')

# Find the accuracy on Validation Dataset
y_pred1 = LS_R.predict(validation_data)
print('\n Validation set Accuracy:' + str(100 * np.mean((y_pred1 == y_train1).astype(float))) + '%')

# Find the accuracy on Testing Dataset
y_pred2 = LS_R.predict(test_data)
print('\n Test set Accuracy:' + str(100 * np.mean((y_pred1 == y_train2).astype(float))) + '%')

#--------------------------------------------------------------------------------------------------------

#Radial basis Function with gamma default
print('Radial basis Function with gamma default')
LS_RD = SVC()
LS_RD.fit(train_data, y_train)

# Find the accuracy on Training Dataset
y_pred = LS_RD.predict(train_data)
print('\n Training set Accuracy:' + str(100 * np.mean((y_pred == y_train).astype(float))) + '%')

# Find the accuracy on Validation Dataset
y_pred1 = LS_RD.predict(validation_data)
print('\n Validation set Accuracy:' + str(100 * np.mean((y_pred1 == y_train1).astype(float))) + '%')

# Find the accuracy on Testing Dataset
y_pred2 = LS_RD.predict(test_data)
print('\n Test set Accuracy:' + str(100 * np.mean((y_pred2 == y_train2).astype(float))) + '%')

#--------------------------------------------------------------------------------------------------------

#Radial basis Function with gamma default and varying C value - (1,10,20,30,··· ,100')
print('Radial basis Function with gamma default  varying value of C (1,10,20,30,··· ,100)')
C = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
for i in C:
    print('C value is' + str(C))
    LS_RDC = SVC(C=i)
    LS_RDC.fit(train_data, y_train)
    
    # Find the accuracy on Training Dataset
    y_pred = LS_RDC.predict(train_data)
    print('\n Training set Accuracy: C=' + str(100 * np.mean((y_pred == y_train).astype(float))) + '%')

    # Find the accuracy on Validation Dataset
    y_pred1 = LS_RDC.predict(validation_data)
    print('\n Validation set Accuracy: C=' + str(100 * np.mean((y_pred1 == y_train1).astype(float))) + '%')

    # Find the accuracy on Testing Dataset
    y_pred2 = LS_RDC.predict(test_data)
    print('\n Test set Accuracy: C=' + str(100 * np.mean((y_pred2 == y_train2).astype(float))) + '%')

#======================================================================================================================
"""
Script for Extra Credit Part
"""
# FOR EXTRA CREDIT ONLY
W_b = np.zeros((n_feature + 1, n_class))
initialWeights_b = np.zeros((n_feature + 1, n_class))
opts_b = {'maxiter': 100}

args_b = (train_data, Y)
nn_params = minimize(mlrObjFunction, initialWeights_b, jac=True, args=args_b, method='CG', options=opts_b)
W_b = nn_params.x.reshape((n_feature + 1, n_class))

# Find the accuracy on Training Dataset
predicted_label_b = mlrPredict(W_b, train_data)
print('\n Training set Accuracy:' + str(100 * np.mean((predicted_label_b == train_label).astype(float))) + '%')

# Find the accuracy on Validation Dataset
predicted_label_b = mlrPredict(W_b, validation_data)
print('\n Validation set Accuracy:' + str(100 * np.mean((predicted_label_b == validation_label).astype(float))) + '%')

# Find the accuracy on Testing Dataset
predicted_label_b = mlrPredict(W_b, test_data)
print('\n Testing set Accuracy:' + str(100 * np.mean((predicted_label_b == test_label).astype(float))) + '%')

with open('params.pickle', 'wb') as f1:
    pickle.dump(W, f1)
with open('params_bonus.pickle', 'wb') as f2:
    pickle.dump(W_b, f2)