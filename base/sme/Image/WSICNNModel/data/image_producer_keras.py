# Databricks notebook source
import numpy as np
import keras


class DataGenerator(keras.utils.Sequence):
    'Generates data for Keras'
    
    def __init__(self, dataloader_tumor, dataloader_normal,batch_size,num_replica,rank):
        """
        Initialize the data producer.

        Arguments:
            data_path: string, path to pre-sampled images using patch_gen.py
            json_path: string, path to the annotations in json format
            img_size: int, size of pre-sampled images, e.g. 768
            batch_size: int, size of batch, e.g. 32
            patch_size: int, size of the patch, e.g. 256
            crop_size: int, size of the final crop that is feed into a CNN,
                e.g. 224 for ResNet
            normalize: bool, if normalize the [0, 255] pixel values to [-1, 1],
                mostly False for debuging purpose
        """
        self._steps = len(dataloader_tumor)
        self._dataloader_tumor = dataloader_tumor
        self._dataloader_normal = dataloader_normal
        self._batch_size = batch_size
        self._grid_size = dataloader_tumor._grid_size
        self._dataiter_tumor = iter(dataloader_tumor)
        self._dataiter_normal = iter(dataloader_normal)
        self.shuffle = True
        self._crop_size = dataloader_tumor._crop_size
        self._num_replica = num_replica
        self._rank = rank
        self.on_epoch_end()
        
    def __len__(self):
        'Denotes the number of batches per epoch'
        return int(np.floor(self._steps / (self._batch_size * self._num_replica)))
                   
    def __getitem__(self, index):
      
        'Generate one batch of data'
        # Generate indexes of the batch
        indexes = self.indexes[index*self._batch_size:(index+1)*self._batch_size]

        # Find list of IDs
        _dataiter_tumor_batch = [self._dataloader_tumor[k] for k in indexes]
        _dataiter_normal_batch = [self._dataloader_normal[k] for k in indexes]
        
        # Generate data
        img_flat1, label_flat1 = self.__data_generation(_dataiter_tumor_batch)
        img_flat2, label_flat2 = self.__data_generation(_dataiter_normal_batch)
        
        idx_rand = np.random.permutation((self._batch_size * 2))
        data =  np.concatenate([img_flat1, img_flat2])[idx_rand]
        target = np.concatenate([label_flat1, label_flat2])[idx_rand]
        
        print(self._num_replica)
        print(self._rank)
        print(index)
        print(self._batch_size)
        print(indexes)
        print(data.shape)
        print(target.shape)
        
        data = data.reshape((-1,3,224,224))
        target = target.reshape((-1,))
        
        print(data.shape)
        print(target.shape)
        
        return (data, target)
      
    def __data_generation(self, _normal_tumor_batch):
      
        # flatten the square grid
        X = np.zeros(
            (self._batch_size,self._grid_size, 3, self._crop_size, self._crop_size),
            dtype=np.float32)
        y = np.zeros((self._batch_size,self._grid_size), dtype=np.float32)

        # Generate data
        for i, ID in enumerate(_normal_tumor_batch):
            # Store sample
            img,tar = ID
            X[i,] = img

            # Store class
            y[i] = tar
          
        return (X, y)

    def on_epoch_end(self):
        'Updates indexes after each epoch'
        self.indexes = np.arange(self._steps)
        self.indexes = self.indexes[self._rank:self._steps:self._num_replica]
        print(self.indexes)
        if self.shuffle == True:
            np.random.shuffle(self.indexes)