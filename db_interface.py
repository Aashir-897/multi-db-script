from abc import ABC, abstractmethod

class DatabaseInterface(ABC):
    @abstractmethod
    def connect(self):
        print("Connecting to database...")

    @abstractmethod    
    def find(self):
        print("Finding records in database...")

    @abstractmethod
    def insert(self):
        print("Inserting records into database...")

    @abstractmethod
    def update(self):
        print  ("Updating records in database...")

    @abstractmethod
    def delete(self):
        print("Deleting records from database...")