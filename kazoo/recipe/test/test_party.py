import unittest
import uuid

from kazoo.recipe.party import ZooParty
from kazoo.test import get_client_or_skip

class ZooPartyTests(unittest.TestCase):
    def setUp(self):
        self._c = get_client_or_skip()
        self._c.connect()
        self.path = "/" + uuid.uuid4().hex

    def tearDown(self):
        if self.path:
            try:
                self._c.recursive_delete(self.path)
            except Exception:
                pass
        if self._c:
            self._c.close()

    def test_party(self):
        parties = [ZooParty(self._c, self.path, "p%s" % i)
                    for i in range(5)]

        one_party = parties[0]

        self.assertEqual(one_party.get_participants(), [])
        self.assertEqual(one_party.get_participant_count(), 0)

        participants = set()
        for party in parties:
            party.join()
            participants.add(party.data)

            self.assertEqual(set(party.get_participants()), participants)
            self.assertEqual(party.get_participant_count(), len(participants))

        for party in parties:
            party.leave()
            participants.remove(party.data)

            self.assertEqual(set(party.get_participants()), participants)
            self.assertEqual(party.get_participant_count(), len(participants))

  