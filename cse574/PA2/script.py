import numpy as np
from scipy.optimize import minimize
from scipy.io import loadmat
from numpy.linalg import det, inv
from math import sqrt, pi ,exp, expm1
import scipy.io
import matplotlib.pyplot as plt
import pickle
import sys

def ldaLearn(X,y):
    # Finding number of output classes
    out_class = set()
    for i in np.nditer(y):
        out_class.add(int(i))

    # Calculating means

    means = np.zeros([X.shape[1],len(out_class)]);
    for label in out_class:
        count = 0;
        for i in range(X.shape[0]):
            if y[i] == label:
                count +=1
                means[:,label-1] =  np.add(means[:,label-1] , X[i].T)
        means[:,label-1] = means[:,label-1]/count
    # print "LDA LEARN"
    # print "means in ldaLearn"
    # print means

    # Calculating covmat
    cov_means = np.sum(X,0)/(int(X.shape[0]))
    covmat = np.dot((X - cov_means).T,X - cov_means) / int(X.shape[0])
    # print "covmat in ldaLearn"
    # print covmat

    return means,covmat

def qdaLearn(X,y):
    # Finding number of output classes
    out_class = set()
    for i in np.nditer(y):
        out_class.add(int(i))

    # Calculating means
    means = np.zeros([X.shape[1],len(out_class)]);
    for label in out_class:
        count = 0;
        for i in range(X.shape[0]):
            if y[i] == label:
                count +=1
                means[:,label-1] =  np.add(means[:,label-1] , X[i].T)
        means[:,label-1] = means[:,label-1]/count
    # print "QDA LEARN"
    # print "means in qdaLearn"
    # print means

    # Calculating Covariance
    covmats = []
    for label in out_class:
        sub_class = np.empty((0,2))
        sub_count = 0
        for k in range(X.shape[0]):
            if y[k]==label:
                temp = X[k].reshape([1,2])
                sub_class = np.append(sub_class,temp,0)
                sub_count = sub_count + 1
        # Covariance mat for each class
        x_minus_u = (sub_class - means[:,label-1].T)
        sub_covmat = np.dot(x_minus_u.T,x_minus_u)/sub_count
        covmats.append(sub_covmat)
    # print "covmats in qdaLearn"
    # print covmats

    return means,covmats

def ldaTest(means,covmat,Xtest,ytest):
    
    out_class = [1,2,3,4,5]
    det_covmat = np.linalg.det(covmat)
    covmat_inv = inv(covmat)
    ypred = []
    denominator = sqrt((2*pi)) * pow(abs(det_covmat),(Xtest.shape[1]/2))

    for i in range(Xtest.shape[0]):
        ypred_values = []
        for label in out_class:
            label = (int)(label-1)
            temp1 =  (Xtest[i]-means[:,label])
            temp2 =  np.dot(np.dot(temp1.T, covmat_inv),temp1)
            exp_value = exp((-1/2)*temp2)
            pred = exp_value/denominator
            ypred_values = np.append(ypred_values, pred)
        class_count = (np.argmax(ypred_values)+1)
        ypred.append(class_count)

    acc_test = 0
    for i in range(0,ytest.shape[0]):
        if int(ytest[i]) == ypred[i]:
            acc_test +=1
    # print "number of correct predictions : " + str(acc_test)
    # print "Accuracy : " + str((acc_test/float(ytest.shape[0]))*100)
    acc = (acc_test/float(ytest.shape[0]))*100

    ypred1 = np.asarray(ypred).reshape(ytest.shape)
    return acc,ypred1

def qdaTest(means,covmats,Xtest,ytest):
    out_class = [1,2,3,4,5]
    det_covmats = []
    covmats_inv = []
    for i in range(len(out_class)):
        det_covmats.append(np.linalg.det(covmats[i]))
        covmats_inv.append(inv(covmats[i]))

    ypred = []

    # Prediction
    for i in range(Xtest.shape[0]):
        ypred_values = []
        for label in out_class:
            label = (int)(label-1)
            denominator = (sqrt(2*pi) * pow((abs(det_covmats[label])),(Xtest.shape[1]/2)))
            temp1 =  (Xtest[i]-means[:,label])
            temp2 =  np.dot(np.dot(temp1.T, covmats_inv[label]),temp1)
            exp_value = exp((-1/2)*temp2)
            pred = exp_value/denominator
            ypred_values = np.append(ypred_values, pred)
        class_count = (np.argmax(ypred_values)+1)
        ypred.append(class_count)

    acc_test = 0
    for i in range(0,ytest.shape[0]):
        if int(ytest[i]) == ypred[i]:
            acc_test +=1
    # print "number of correct predictions : " + str(acc_test)
    # print "Accuracy : " + str((acc_test/float(ytest.shape[0]))*100)
    acc = (acc_test/float(ytest.shape[0]))*100
    ypred1 = np.asarray(ypred).reshape(ytest.shape)
    return acc, ypred1

def learnOLERegression(X,y):
    
    w_temp1 = inv(np.dot(X.T, X))
    w_temp2 = np.dot(X.T, y)
    w = np.dot(w_temp1, w_temp2)
    return w

def learnRidgeRegression(X,y,lambd):
    
    d = X.shape[1]
    lam_iden = lambd * np.eye(d)
    temp1 = inv(lam_iden + np.dot(X.T, X))
    temp2 = np.dot(X.T, y)
    w = np.dot(temp1, temp2)
    return w

def testOLERegression(w,Xtest,ytest):
    
    N = Xtest.shape[0]
    temp1 = np.dot(w.T,Xtest.T)
    temp2 = ytest - temp1.T
    # print " INSIDE Test OLE"
    # print temp1.shape
    # print temp2.shape
    temp2 = np.power(temp2,2)
    sum = (np.sum(temp2,0))/float(N)
    rmse = sqrt(sum)
    # print rmse
    return rmse

def regressionObjVal(w, X, y, lambd):
    
    w = w.reshape((X.shape[1],1))
    value = (y - np.dot(X,w).reshape((y.shape)))
    temp1 = np.dot(value.T, value)
    temp2 = np.dot(w.T, w)
    error1 = (temp1 + (lambd * temp2))/2
    error = np.sum(error1, axis=0)
    #Calculating Gradient of error
    term_1 = np.dot(np.dot(X.T, X), w).reshape((w.shape[0],1))
    term_2 = np.dot(X.T, y).reshape((w.shape[0],1))
    term_3 = lambd * w.reshape((w.shape[0],1))    
    
    tamp = (term_1 - term_2 + term_3)
    error_grad = tamp.flatten()
    # print error_grad

    return error, error_grad

def mapNonLinear(x,p):

    N = x.shape[0]
    Xd = np.array([])
    Xd_temp = np.array([])
    if(p==0):
            Xd = np.ones((N,1))
    else:
        for i in range(x.shape[0]):
            X_temp = []
            x_i = x[i]
            for power in range(p+1):
                term = pow(x_i, power)
                X_temp = np.append(X_temp, term)
            Xd_temp = np.append(Xd_temp, X_temp)
        Xd = Xd_temp.reshape((N,p+1))
    #print Xd.shape      
    return Xd
    

# Main script

# Problem 1
# load the sample data
if sys.version_info.major == 2:
    X,y,Xtest,ytest = pickle.load(open('sample.pickle','rb'))
else:
    X,y,Xtest,ytest = pickle.load(open('sample.pickle','rb'),encoding = 'latin1')

# LDA
means,covmat = ldaLearn(X,y)
ldaacc = ldaTest(means,covmat,Xtest,ytest)
print('LDA Accuracy = '+str(ldaacc))
# QDA
means,covmats = qdaLearn(X,y)
qdaacc = qdaTest(means,covmats,Xtest,ytest)
print('QDA Accuracy = '+str(qdaacc))

# plotting boundaries
x1 = np.linspace(-5,20,100)
x2 = np.linspace(-5,20,100)
xx1,xx2 = np.meshgrid(x1,x2)
xx = np.zeros((x1.shape[0]*x2.shape[0],2))
xx[:,0] = xx1.ravel()
xx[:,1] = xx2.ravel()

zacc,zldares = ldaTest(means,covmat,xx,np.zeros((xx.shape[0],1)))
plt.contourf(x1,x2,zldares.reshape((x1.shape[0],x2.shape[0])))
plt.scatter(Xtest[:,0],Xtest[:,1],c=ytest)
plt.show()

zacc,zqdares = qdaTest(means,covmats,xx,np.zeros((xx.shape[0],1)))
plt.contourf(x1,x2,zqdares.reshape((x1.shape[0],x2.shape[0])))
plt.scatter(Xtest[:,0],Xtest[:,1],c=ytest)
plt.show()

# Problem 2
if sys.version_info.major == 2:
    X,y,Xtest,ytest = pickle.load(open('diabetes.pickle','rb'))
else:
    X,y,Xtest,ytest = pickle.load(open('diabetes.pickle','rb'),encoding = 'latin1')

# add intercept
X_i = np.concatenate((np.ones((X.shape[0],1)), X), axis=1)
Xtest_i = np.concatenate((np.ones((Xtest.shape[0],1)), Xtest), axis=1)

w = learnOLERegression(X,y)
mle = testOLERegression(w,Xtest,ytest)

w_i = learnOLERegression(X_i,y)
mle_i = testOLERegression(w_i,Xtest_i,ytest)

print('RMSE without intercept '+str(mle))
print('RMSE with intercept '+str(mle_i))

# Problem 3 with Test data
k = 101
lambdas = np.linspace(0, 1, num=k)
i = 0
rmses3_test = np.zeros((k,1))
for lambd in lambdas:
    w_l = learnRidgeRegression(X_i,y,lambd)
    rmses3_test[i] = testOLERegression(w_l, Xtest_i, ytest)
    #rmses3_test[i] = testOLERegression(w_l,X_i,y)
    i = i + 1
print "rmses3_test:"
print rmses3_test
plt.plot(lambdas,rmses3_test)
plt.show()

# Problem 3 with training data
k = 101
lambdas = np.linspace(0, 1, num=k)
i = 0
rmses3_train = np.zeros((k,1))
for lambd in lambdas:
    w_l = learnRidgeRegression(X_i,y,lambd)
    rmses3_train[i] = testOLERegression(w_l, X_i, y)
    i = i + 1
print "rmses3_train :"
print rmses3_train
plt.plot(lambdas,rmses3_train)
plt.show()

# Problem 4 with test
k = 101
lambdas = np.linspace(0, 1, num=k)
i = 0
rmses4_test = np.zeros((k,1))
opts = {'maxiter' : 100}    # Preferred value.
w_init = np.ones((X_i.shape[1],1))
print "W_init shape"+str(w_init.shape)
for lambd in lambdas:
    args = (X_i, y, lambd)
    w_l = minimize(regressionObjVal, w_init, jac=True, args=args,method='CG', options=opts)
    w_l = np.transpose(np.array(w_l.x))
    w_l = np.reshape(w_l,[len(w_l),1])
    rmses4_test[i] = testOLERegression(w_l,Xtest_i,ytest)
    i = i + 1
print "Difference"
print np.sum(w_init-w_l, axis = 0)

print "RMSES4 test:"
print rmses4_test
plt.plot(lambdas,rmses4_test)
plt.show()
# Problem 4 with training
k = 101
lambdas = np.linspace(0, 1, num=k)
i = 0
rmses4_train = np.zeros((k,1))
opts = {'maxiter' : 100}    # Preferred value.
w_init = np.ones((X_i.shape[1],1))
print "W_init shape"+str(w_init.shape)
for lambd in lambdas:
    args = (X_i, y, lambd)
    w_l = minimize(regressionObjVal, w_init, jac=True, args=args,method='CG', options=opts)
    w_l = np.transpose(np.array(w_l.x))
    w_l = np.reshape(w_l,[len(w_l),1])
    rmses4_train[i] = testOLERegression(w_l,X_i,y)
    i = i + 1
print "Difference"
print np.sum(w_init-w_l, axis = 0)

print "RMSES4 Training :"
print rmses4_train
plt.plot(lambdas,rmses4_train)
plt.show()


# Problem 5 Test optimal lambda vs zero lambda
pmax = 7
lambda_opt = lambdas[np.argmin(rmses4_test)]
rmses5_test = np.zeros((pmax,2))
for p in range(pmax):
    Xd = mapNonLinear(X[:,2],p)
    Xdtest = mapNonLinear(Xtest[:,2],p)
    w_d1 = learnRidgeRegression(Xd,y,0)
    rmses5_test[p,0] = testOLERegression(w_d1,Xdtest,ytest)
    w_d2 = learnRidgeRegression(Xd,y,lambda_opt)
    rmses5_test[p,1] = testOLERegression(w_d2,Xdtest,ytest)
print "RMSES5 Test:"
print rmses5_test
plt.plot(range(pmax),rmses5_test)
plt.legend(('No Regularization','Regularization'))
plt.show()

# Problem 5 Train optimal lambda vs zero lambda
pmax = 7
lambda_opt = lambdas[np.argmin(rmses4_test)]
rmses5_train = np.zeros((pmax,2))
for p in range(pmax):
    Xd = mapNonLinear(X[:,2],p)
    Xdtest = mapNonLinear(Xtest[:,2],p)
    w_d1 = learnRidgeRegression(Xd,y,0)
    rmses5_train[p,0] = testOLERegression(w_d1,Xd,y)
    w_d2 = learnRidgeRegression(Xd,y,lambda_opt)
    rmses5_train[p,1] = testOLERegression(w_d2,Xd,y)
print "RMSES5 Train:"
print rmses5_train
plt.plot(range(pmax),rmses5_train)
plt.legend(('No Regularization','Regularization'))
plt.show()
