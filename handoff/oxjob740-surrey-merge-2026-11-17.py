"""#740 (ZD 22240, Surrey): one repository source for the University of Surrey. Support factory #2254, written off desk
2026-11-17 (lane #2263); run ON DESK from the openalex-sources checkout. Reuses merge()/rename()/upd()/snapshot() from
#629's tested handoff script, as #883's registry_after_night_2026-11-16.py did.

Why (live API 17 Nov 12:30 CT, receipts in #740 work/surrey_sources_2026-11-17.json):
  - S7407059866 'University of Surrey Open Research repository' (homepage openresearch.surrey.ac.uk/esploro/, no
    institution) carries Surrey's current Esploro feed, endpoint 27r4ivz7htjtuezwc76o (#1225 moved it there 16 Nov
    13:20 CT): 86,310 works (91,228 with xpac).
  - S4306400680 'Surrey Research Insight Open Access (The University of Surrey)' (institution University of Surrey)
    holds the records of Surrey's retired EPrints repository epubs.surrey.ac.uk: 53,511 works; 2 published since 2025.
    Every sampled epubs link 302s to the same item in Esploro, and 99 of 100 sampled works also carry the Esploro copy.
  - A support ticket (9 Nov) asked whether the duplicate Surrey source records should be consolidated.
  Same repository, two records: merge the newer, institution-less record into the older, institution-linked one
  (#1287 rule: keep the linked source; #883's 15 Nov plan bound this feed to S4306400680 for the same reason), and give
  the winner the repository's current name and homepage (D5 2026-07-17: current name wins; the old name stays as an
  alternate title). Merged ids resolve to the winner on the API (checked 17 Nov on S4306400651 -> S4306401300).

What it does, in ONE transaction (dry run by default, rolled back):
  1. Snapshot both sources (printed in the receipt's `before`; read them in the dry run).
  2. Guards: both exist and are repositories; names as expected; loser not curator-overridden (merge() asserts);
     endpoint 27r4ivz7htjtuezwc76o is bound to the loser.
  3. merge(7407059866 -> 4306400680): moves the loser's endpoints (the Esploro feed) to the winner, adds the loser's
     name as an alternate title, writes a source_merge row, deletes the loser.
  4. rename 4306400680 -> 'University of Surrey Open Research repository'; homepage_url ->
     https://openresearch.surrey.ac.uk/esploro/ .
  5. After-guard: 27r4ivz7htjtuezwc76o is bound to 4306400680.

Size: works re-stamped on the night it rides ~ the union of both sources' works, at most 148,281 (91,228 + 57,053 with
xpac; real overlap is high, ~92K). Location source ids/names change only; no OA change (both repositories, is_oa false).
Pre-clear recorded by the lane (oxpreclear --max-changed 150000, night 2026-11-18). Run before 21:30 CT on 17 Nov to
ride night 2026-11-18 (run 18 Nov 05:00 UTC); later than that: it rides the next night, re-pre-clear for that night.
Ran on desk 21 Nov 2026 (nights 11-18 to 11-21 missed): rides night 2026-11-22, pre-cleared with preclear_e2e.py --add 180K.

Run on desk:
    cp <this file without .txt> ~/ox/openalex-sources/handoff/oxjob740-surrey-merge-2026-11-17.py
    cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob740-surrey-merge-2026-11-17.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob740-surrey-merge-2026-11-17.py --execute
Save the RECEIPT JSON as #740 work/oxjob740-surrey-merge-2026-11-17_receipt.json.
Revert (from the receipt's `before`): re-insert the loser's row 7407059866 (its datacite_id/datacite_ids too);
UPDATE oai_pmh_endpoint SET source_id = 7407059866 WHERE id IN ('27r4ivz7htjtuezwc76o', 'mwunwhjjejf3oa8sqf4s');
UPDATE source_datacite_id SET source_id = 7407059866 WHERE datacite_id IN (receipt `before_datacite`, i.e. 'bl.surrey');
UPDATE source_merge SET winner_id = 7407059866 WHERE id IN (receipt `merge_chain_repointed`);
DELETE FROM source_merge WHERE loser_id = 7407059866 AND rule = 'zendesk_740_ticket_merge'; set 4306400680's
display_name, display_name_before_override, override_timestamp, alternate_titles, homepage_url, datacite_id and
datacite_ids back to their `before` values.

Check after the night: https://api.openalex.org/sources/S7407059866 resolves to S4306400680 named 'University of Surrey
Open Research repository'; https://api.openalex.org/works?filter=locations.source.id:S4306400680,from_publication_date:2025-01-01
~3,900 (was 2); W3172019206's Surrey copy (pmh:oai:alma.44SUR_INST:11149879690002346) shows source S4306400680.
"""
import importlib.util
import json
import os
import sys

P629 = os.path.expanduser('~/ox/openalex-sources/handoff/oxjob629-ticket-merges-2026-11-15.py')
spec = importlib.util.spec_from_file_location('m629', P629)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)  # defines helpers only; its main() runs under __main__
from sqlalchemy import text  # noqa: E402

EXECUTE = '--execute' in sys.argv
m.receipt.update({'job': 740, 'tickets': [22240], 'execute': EXECUTE, 'before': {}, 'actions': []})
LOSER, WINNER = 7407059866, 4306400680
SURREY_EP = '27r4ivz7htjtuezwc76o'
NEW_NAME = 'University of Surrey Open Research repository'
NEW_HOMEPAGE = 'https://openresearch.surrey.ac.uk/esploro/'


def main():
    try:
        with m.engine.begin() as conn:
            m.snapshot(conn, {LOSER, WINNER})
            src = {r.id: r for r in conn.execute(text(
                'SELECT id, display_name, type, institution_id FROM sources WHERE id = ANY(:i)'), {'i': [LOSER, WINNER]})}
            assert LOSER in src and WINNER in src, sorted(src)
            assert src[LOSER].type == 'repository' and src[WINNER].type == 'repository', {k: v.type for k, v in src.items()}
            assert src[LOSER].display_name == NEW_NAME, src[LOSER].display_name
            assert 'Surrey Research Insight' in src[WINNER].display_name, src[WINNER].display_name
            m.log(f'winner institution_id {src[WINNER].institution_id}; loser institution_id {src[LOSER].institution_id}')
            ep = conn.execute(text('SELECT source_id, pmh_url, pmh_set, metadata_prefix FROM oai_pmh_endpoint WHERE id = :e'),
                              {'e': SURREY_EP}).fetchone()
            assert ep is not None and ep.source_id == LOSER, ep
            m.log(f'endpoint {SURREY_EP} before: {dict(ep._mapping)}')
            # chain collapse (as #654's script): earlier merges into the loser now point at the winner
            m.receipt['merge_chain_repointed'] = []
            for mid, old_loser in conn.execute(text('SELECT id, loser_id FROM source_merge WHERE winner_id = :l'),
                                               {'l': LOSER}).fetchall():
                conn.execute(text('UPDATE source_merge SET winner_id = :w WHERE id = :m'), {'w': WINNER, 'm': mid})
                m.receipt['merge_chain_repointed'].append({'source_merge_id': mid, 'loser_id': old_loser})
                m.log(f'source_merge {mid}: {old_loser} -> {LOSER} re-pointed to {WINNER}')
            m.receipt['before_datacite'] = [r[0] for r in conn.execute(
                text('SELECT datacite_id FROM source_datacite_id WHERE source_id = :l'), {'l': LOSER})]
            m.merge(conn, LOSER, WINNER, 'zendesk_740_ticket_merge',
                    {'tickets': [22240], 'oxjob': 740, 'approved_by': 'support factory #2254 (standing authority)',
                     'note': 'S4306400680 holds retired EPrints epubs.surrey.ac.uk records (links 302 to Esploro); '
                             'S7407059866 holds the current Esploro feed: one repository'})
            m.rename(conn, WINNER, NEW_NAME)
            m.upd(conn, WINNER, homepage_url=NEW_HOMEPAGE)
            after = conn.execute(text('SELECT source_id FROM oai_pmh_endpoint WHERE id = :e'), {'e': SURREY_EP}).scalar()
            assert after == WINNER, after
            m.log(f'endpoint {SURREY_EP} now on {WINNER}')
            if not EXECUTE:
                raise m.DryRun()
            m.log('COMMITTED')
    except m.DryRun:
        m.log('DRY RUN - rolled back')
    print('RECEIPT')
    print(json.dumps(m.receipt, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
