import numpy as np
import skimage
import pickle

def unpickle(file):
    fo = open(file, 'rb')
    dict = pickle.load(fo, encoding='latin1')
    fo.close()
    return dict

def read_data(directory, is_train):
    names = unpickle('{}/batches.meta'.format(directory))['label_names']

    data, labels = [], []

    if is_train:
        for i in range(1, 6):
            filename = '{}/data_batch_{}'.format(directory, i)

            batch_data = unpickle(filename)
            if len(data) > 0:
                data = np.vstack((data, batch_data['data']))
                labels = np.hstack((labels, batch_data['labels']))
            else:
                data = batch_data['data']
                labels = batch_data['labels']
    else:
        filename = '{}/test_batch'.format(directory)
        batch_data = unpickle(filename)

        data = np.array(batch_data['data'])
        labels = np.array(batch_data['labels'])

    return names, data, labels

def scale_imgs(data):
    # 3072 = 3 * 32 * 32
    # origional image size = (32 x 32)
    # each pixel contain a list of (R, G, B)
    imgs = np.reshape(data, (data.shape[0], 3, 32, 32))

    # tranpose, allow displaying with matplotlib
    proper_imgs = np.transpose(imgs, (0,2,3,1))

    imgs_norm = np.divide(proper_imgs, 255.0) # normalize RGBs

    # # crop to 30 x 30 image, from center
    # cropped_imgs = imgs_norm[:, 2:30, 2:30, :]

    resized_img = skimage.transform.resize(imgs_norm, (data.shape[0], 224, 224))
    # resized_img = cv2.resize(cropped_imgs, (data.shape[0], 224, 224))

    # calculate mean of (R, G, B)
    # grayscale_imgs = np.reshape(
    #     np.mean(resized_img, axis=3),
    #     (data.shape[0], 224, 224, 1))

    # mean = np.mean(grayscale_imgs)
    # std_deviation = np.std(grayscale_imgs)

    # norm = np.divide(np.subtract(grayscale_imgs, mean), std_deviation)

    return resized_img

def scale_img(data):
    # 3072 = 3 * 32 * 32
    # origional image size = (32 x 32)
    # each pixel contain a list of (R, G, B)
    imgs = np.reshape(data, (3, 32, 32))

    # tranpose, allow displaying with matplotlib
    proper_imgs = np.transpose(imgs, (1,2,0))

    imgs_norm = np.divide(proper_imgs, 255.0) # normalize RGBs

    # # crop to 30 x 30 image, from center
    # cropped_imgs = imgs_norm[:, 1:31, 1:31, :]

    resized_img = skimage.transform.resize(imgs_norm, (224, 224))
    # resized_img = cv2.resize(cropped_imgs, (224, 224))

    # calculate mean of (R, G, B)
    # grayscale_imgs = np.reshape(
    #     np.mean(resized_img, axis=2),
    #     (224, 224, 1))
    
    # mean = np.mean(grayscale_imgs)
    # std_deviation = np.std(grayscale_imgs)

    # norm = np.divide(np.subtract(grayscale_imgs, mean), std_deviation)

    return resized_img
