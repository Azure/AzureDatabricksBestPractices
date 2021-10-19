# Databricks notebook source
from somepackage.mymodule1 import MyClass

class MyOtherClass(object):
  def helloWorld(self):
    return 'hello!'
  
  def nestedImport(self):
    return MyClass().foo()