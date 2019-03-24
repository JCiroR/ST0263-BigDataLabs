# coding=utf-8
# Salario promedio por sector económico
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordFrequencyCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_employees_by_sector,
                    reducer=self. reducer_delete_repeated_sectors),
            MRStep(reducer=self.reducer_sectors_by_employee)
        ]

    def mapper_employees_by_sector(self, _, line):
        line = line.split(',')
        if not line[0].isdigit(): return
        yield line[1], line[0]

    def reducer_delete_repeated_sectors(self, key, values):
        for employee in values:
            yield employee, key

    def reducer_sectors_by_employee(self, key, values):
        yield key, sum(1  for _ in values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()