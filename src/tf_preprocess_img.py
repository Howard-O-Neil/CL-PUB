import tensorflow_core as tf

def tf_preprocess_images(data, is_train):
    data_f = tf.cast(data, tf.float32)
    data_shape = tf.shape(data_f)
    data_size = tf.squeeze(tf.slice(data_shape, [0], [1]))

    # 3072 = 3 * 32 * 32
    # origional image size = (32 x 32)
    # each pixel contain a list of (R, G, B)
    imgs = tf.reshape(data_f, [data_size, 3, 32, 32])

    # tranpose, allow displaying with matplotlib
    proper_imgs = tf.transpose(imgs, [0,2,3,1])

    imgs_norm = tf.divide(proper_imgs, 255.0) # normalize RGBs

    gray_scale = tf.reshape(
        tf.reduce_mean(
            tf.image.resize_images(
                imgs_norm,
                size=[224, 224], method=tf.image.ResizeMethod.BILINEAR
            ), axis=3), \
        [-1, 224, 224, 1]
    )

    if is_train:
        gray_scale = imgs_norm

    return gray_scale

def gray_scaling(imgs):
    return tf.reshape(tf.reduce_mean( \
        tf.image.resize_images( \
                imgs, \
                size=[224, 224], method=tf.image.ResizeMethod.BILINEAR \
            ), axis=3), [-1, 224, 224, 1])

def tf_augment_images(data):
    random_rotations = tf.concat([ \
            gray_scaling(data), \
            gray_scaling( \
                tf.contrib.image.rotate( \
                    data, tf.random.uniform([1], minval=-0.5235988, maxval=0.5235988), interpolation='BILINEAR', name=None)) \
        ], 0)
    
    flip_left_right = tf.concat([random_rotations, gray_scaling(tf.image.flip_left_right(data))], 0)

    return flip_left_right
