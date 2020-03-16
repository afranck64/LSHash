import random
import string
from unittest import TestCase
from unittest.mock import patch
from fakeredis import FakeStrictRedis, FakeRedis
from pprint import pprint
import sys
import os

# add the LSHash package to the current python path
sys.path.insert(0, os.path.abspath('../'))
# now we can use our lshash package and not the standard one
from lshash import LSHash, MultiLevelLSHash

NB_ELEMENTS = 100
HASH_SIZE = 16
INPUT_DIM = 128

ELEMENTS = []
ELEMENTS_NAMES = []


for _ in range(NB_ELEMENTS):
    el = [random.random()*2-1 for _ in range(INPUT_DIM)]
    elname = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
    ELEMENTS.append(tuple(el))
    ELEMENTS_NAMES.append(elname)

class TestLSHash(TestCase):
    nb_elements = NB_ELEMENTS
    hash_size = HASH_SIZE
    input_dim = INPUT_DIM
    els = ELEMENTS
    el_names = ELEMENTS_NAMES

    def test_lshash(self):
        lsh = LSHash(self.hash_size, self.input_dim, 1)
        for i in range(self.nb_elements):
            lsh.index(list(self.els[i]))
            lsh.index(list(self.els[i]))  # multiple insertions
        hasht = lsh.hash_tables[0]
        itms = [hasht.get_list(k) for k in hasht.keys()]
        for itm in itms:
            self.assertEqual(itms.count(itm), 1)
            for el in itm:
                self.assertIn(el, self.els)
        for el in self.els:
            res = lsh.query(list(el), num_results=1, distance_func='euclidean')[0]
            # res is a tuple containing the vector and the distance
            el_v, el_dist = res
            self.assertIn(el_v, self.els)
            self.assertEqual(el_dist, 0)
        del lsh

    def test_lshash_extra_val(self):
        lsh = LSHash(self.hash_size, self.input_dim, 1, storage_config={'dict': None})
        for i in range(self.nb_elements):
            lsh.index(list(self.els[i]), self.el_names[i])
        hasht = lsh.hash_tables[0]
        itms = [hasht.get_list(k) for k in hasht.keys()]
        for itm in itms:
            for el in itm:
                self.assertIn(el[0], self.els)
                self.assertIn(el[1], self.el_names)
        for el in self.els:
            # res is a list, so we need to select the first entry only
            res = lsh.query(list(el), num_results=1, distance_func='euclidean')[0]
            # vector an name are in the first element of the tuple res[0]
            el_v, el_name = res[0]
            # the distance is in the second element of the tuple
            el_dist = res[1]
            self.assertIn(el_v, self.els)
            self.assertIn(el_name, self.el_names)
            self.assertEqual(el_dist, 0)
        del lsh

class TestLSHashSQLite(TestCase):
    nb_elements = NB_ELEMENTS
    hash_size = HASH_SIZE
    input_dim = INPUT_DIM
    els = ELEMENTS
    el_names = ELEMENTS_NAMES

    def test_lshash_sqlite(self):
        lsh = LSHash(self.hash_size, self.input_dim, 1, storage_config={"sqlite": None})
        for i in range(self.nb_elements):
            lsh.index(list(self.els[i]))
            lsh.index(list(self.els[i]))  # multiple insertions
        hasht = lsh.hash_tables[0]
        itms = [hasht.get_list(k) for k in hasht.keys()]
        for itm in itms:
            self.assertEqual(itms.count(itm), 1)
            for el in itm:
                self.assertIn(el, self.els)
        for el in self.els:
            res = lsh.query(list(el), num_results=1, distance_func='euclidean')[0]
            # res is a tuple containing the vector and the distance
            el_v, el_dist = res
            self.assertIn(el_v, self.els)
            self.assertEqual(el_dist, 0)
        del lsh

    def test_lshash_sqlite_extra_val(self):
        lsh = LSHash(self.hash_size, self.input_dim, 1, storage_config={"sqlite": {'serializer': 'pickle'}})
        for i in range(self.nb_elements):
            lsh.index(list(self.els[i]), self.el_names[i])
        hasht = lsh.hash_tables[0]
        itms = [hasht.get_list(k) for k in hasht.keys()]
        for itm in itms:
            for el in itm:
                self.assertIn(el[0], self.els)
                self.assertIn(el[1], self.el_names)
        for el in self.els:
            # res is a list, so we need to select the first entry only
            res = lsh.query(list(el), num_results=1, distance_func='euclidean')[0]
            # vector an name are in the first element of the tuple res[0]
            el_v, el_name = res[0]
            # the distance is in the second element of the tuple
            el_dist = res[1]
            self.assertIn(el_v, self.els)
            self.assertIn(el_name, self.el_names)
            self.assertEqual(el_dist, 0)
        del lsh

@patch('redis.Redis', FakeRedis)
@patch('redis.StrictRedis', FakeStrictRedis)
class TestLSHashRedis(TestCase):
    nb_elements = NB_ELEMENTS
    hash_size = HASH_SIZE
    input_dim = INPUT_DIM
    els = ELEMENTS
    el_names = ELEMENTS_NAMES

    def test_lshash_redis(self):
        """
        Test external lshash module
        """
        config = {"redis": {"host": 'localhost', "port": 6379, "db": 15}}
        lsh = LSHash(self.hash_size, self.input_dim, 1, config)
        for i in range(self.nb_elements):
            lsh.index(list(self.els[i]))
            lsh.index(list(self.els[i]))  # multiple insertions should be prevented by the library

        hasht = lsh.hash_tables[0]
        itms = [hasht.get_list(k) for k in hasht.keys()]

        for itm in itms:
            for el in itm:
                assert itms.count(itm) == 1  # have multiple insertions been prevented?
                assert el in self.els

        for el in self.els:
            res = lsh.query(list(el), num_results=1, distance_func='euclidean')[0]
            el_v, el_dist = res
            assert el_v in self.els
            assert el_dist == 0
        del lsh

    def test_lshash_redis_extra_val(self):
        """
        Test external lshash module
        """
        config = {"redis": {"host": 'localhost', "port": 6379, "db": 15}}
        lsh = LSHash(self.hash_size, self.input_dim, 1, config)
        for i in range(self.nb_elements):
            lsh.index(list(self.els[i]), self.el_names[i])
            lsh.index(list(self.els[i]), self.el_names[i])  # multiple insertions
        hasht = lsh.hash_tables[0]
        itms = [hasht.get_list(k) for k in hasht.keys()]
        for itm in itms:
            assert itms.count(itm) == 1
            for el in itm:
                assert el[0] in self.els
                assert el[1] in self.el_names
        for el in self.els:
            res = lsh.query(list(el), num_results=1, distance_func='euclidean')[0]
            # vector an name are in the first element of the tuple res[0]
            el_v, el_name = res[0]
            # the distance is in the second element of the tuple
            el_dist = res[1]
            assert el_v in self.els
            assert el_name in self.el_names
            assert el_dist == 0
        del lsh


class TestMultilevelLSHash(TestCase):
    nb_elements = NB_ELEMENTS
    hash_size = HASH_SIZE
    input_dim = INPUT_DIM
    els = ELEMENTS
    el_names = ELEMENTS_NAMES

    def test_lshash_sqlite_multi_levels(self):
        lsh = MultiLevelLSHash(self.hash_size, self.input_dim, 1, storage_config={'sqlite': {'database': ':memory:'}} )
        for i in range(self.nb_elements):
            lsh.index(list(self.els[i]))
            lsh.index(list(self.els[i]))  # multiple insertions
        hasht = lsh.hash_tables[0]
        return
        itms = [hasht.get_list(k) for k in hasht.keys()]
        for itm in itms:
            self.assertEqual(itms.count(itm), 1)
            for el in itm:
                self.assertIn(el, self.els)
        for el in self.els:
            res = lsh.query(list(el), num_results=1, distance_func='euclidean')[0]
            # res is a tuple containing the vector and the distance
            el_v, el_dist = res
            self.assertIn(el_v, self.els)
            self.assertEqual(el_dist, 0)
        del lsh

    def test_lshash_extra_val(self):
        return
        lsh = MultiLevelLSHash(self.hash_size, self.input_dim, 1, storage_config=None )
        for i in range(self.nb_elements):
            lsh.index(list(self.els[i]), self.el_names[i])
        hasht = lsh.hash_tables[0]
        itms = [hasht.get_list(k) for k in hasht.keys()]
        for itm in itms:
            for el in itm:
                self.assertIn(el[0], self.els)
                self.assertIn(el[1], self.el_names)
        for el in self.els:
            # res is a list, so we need to select the first entry only
            res = lsh.query(list(el), num_results=1, distance_func='euclidean')[0]
            # vector an name are in the first element of the tuple res[0]
            el_v, el_name = res[0]
            # the distance is in the second element of the tuple
            el_dist = res[1]
            self.assertIn(el_v, self.els)
            self.assertIn(el_name, self.el_names)
            self.assertEqual(el_dist, 0)
        del lsh
