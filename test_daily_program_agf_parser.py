import unittest

import api_server as api


class DailyProgramAgfParserTests(unittest.TestCase):
    def _horse(self, agf_html):
        table = api.BeautifulSoup(
            f"""
            <table><tbody><tr>
              <td class="gunluk-GunlukYarisProgrami-SiraId">4</td>
              <td class="gunluk-GunlukYarisProgrami-AtAdi">
                <a href="/TR/AtKosuBilgileri?AtId=42">MESTAN</a>
              </td>
              <td class="gunluk-GunlukYarisProgrami-AGFORAN">{agf_html}</td>
            </tr></tbody></table>
            """,
            "html.parser",
        ).find("table")
        horses = api._parse_daily_horses(table)
        self.assertEqual(len(horses), 1)
        return horses[0]

    def test_exact_anchor_title_is_preferred_over_rounded_display(self):
        horse = self._horse('<a title="%8,33(5)">%8(5)</a>')

        self.assertEqual(horse["agf"], "%8(5)")
        self.assertEqual(horse["agfDisplay"], "%8(5)")
        self.assertEqual(api.parse_agf_percent(horse["agf"]), 8.0)
        self.assertEqual(
            horse["agfPools"],
            [
                {
                    "poolNo": None,
                    "percent": 8.33,
                    "rank": 5,
                    "raw": "%8,33(5)",
                    "display": "%8(5)",
                    "source": "title",
                }
            ],
        )

    def test_two_six_leg_pools_are_preserved_without_changing_primary_semantics(self):
        horse = self._horse(
            """
            <a title="1. 6'LI GANYAN : %16,86(2)">%17(2)</a>
            <a title="2. 6'LI GANYAN : %16,07(2)"><br/>%16(2)</a>
            """
        )

        # Legacy visible scoring keeps the rounded cell text; exact values are
        # available only in the structured, ranking-off market payload.
        self.assertEqual(horse["agf"], "%17(2) %16(2)")
        self.assertEqual(horse["agfDisplay"], "%17(2) %16(2)")
        self.assertEqual(api.parse_agf_percent(horse["agf"]), 17.0)
        self.assertEqual(
            [(pool["poolNo"], pool["percent"], pool["rank"]) for pool in horse["agfPools"]],
            [(1, 16.86, 2), (2, 16.07, 2)],
        )

    def test_missing_market_data_stays_fail_closed(self):
        horse = self._horse("<span>-</span>")

        self.assertEqual(horse["agf"], "-")
        self.assertEqual(horse["agfDisplay"], "-")
        self.assertEqual(horse["agfPools"], [])
        self.assertIsNone(api.parse_agf_percent(horse["agf"]))

    def test_malformed_title_falls_back_to_parseable_visible_value(self):
        horse = self._horse('<a title="AGF verisi">%7(4)</a>')

        self.assertEqual(horse["agf"], "%7(4)")
        self.assertEqual(api.parse_agf_percent(horse["agf"]), 7.0)
        self.assertEqual(horse["agfPools"][0]["source"], "display")


if __name__ == "__main__":
    unittest.main()
