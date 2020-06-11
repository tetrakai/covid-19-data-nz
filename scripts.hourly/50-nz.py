#!/usr/bin/env python2

import collections
import copy
import datetime
import json
import re
import os

import bs4
import requests
from word2number import w2n

def main():
  timeseries_data = get_timeseries_data('https://www.health.govt.nz/news-media/media-releases')
  timeseries_data = add_manual_data(timeseries_data)
  timeseries_data = fill_in_blanks(timeseries_data)

  # Muck with the data to get it into the format that's expected
  # Fill in the blanks
  dates = sorted(timeseries_data.keys())

  start_time = min([datetime.datetime.strptime(d, '%Y-%m-%d') for d in dates])
  end_time = max([datetime.datetime.strptime(d, '%Y-%m-%d') for d in dates])

  curr_time = start_time
  prev_time = None
  while curr_time <= end_time:
    key = curr_time.strftime('%Y-%m-%d')

    if key not in timeseries_data:
      timeseries_data[key] = timeseries_data[prev_time.strftime('%Y-%m-%d')]

    prev_time = curr_time
    curr_time = curr_time + datetime.timedelta(days=1)

  dates = sorted(timeseries_data.keys())
  values = [timeseries_data[d] for d in dates]

  # Muck with the age groups and sources data to do the right things
  source_data = munge_data_to_output(timeseries_data, dates, 'sources')

  formatted_data = {
    'timeseries_dates': dates,
    'total': {
      'confirmed': [timeseries_data[d]['confirmed'] for d in dates],
      'recovered': [timeseries_data[d]['recovered'] for d in dates],
      'deaths': [timeseries_data[d].get('deaths', None) for d in dates],
      'tested': [timeseries_data[d].get('tested', None) for d in dates],
      'current_hospitalized': [timeseries_data[d].get('hospitalized', None) for d in dates],
      'current_icu': [timeseries_data[d].get('icu', None) for d in dates],
    },
    'sources': source_data,
  }

  with open('nzl.json', 'w') as f:
    json.dump(formatted_data, f, indent=2, sort_keys=True)

def get_timeseries_data(base_url):
  data = {}

  post_list = []

  page_num = 0
  current_year = '2020'

  # We don't care about posts from before 2020
  while current_year == '2020':
    page = bs4.BeautifulSoup(requests.get(base_url + '?page=%d' % page_num).text, 'html.parser')
    content = page.select_one('div.view-content')

    for li in content.select('div.item-list li'):
      title_div = li.select_one('div.views-field-title')
      news_type = li.select_one('div.views-field-field-news-type')

      if ('COVID-19' in title_div.text or 'new cases' in title_div.text) and news_type.text.strip() == 'Media release' and 'Point of Care Test Kits' not in title_div.text and 'no live media update' not in title_div.text and 'testing system' not in title_div.text:
        post_list.append('https://www.health.govt.nz%s' % title_div.select_one('a').attrs['href'])

      current_year = li.select_one('span.date-display-single').attrs['content'].split('-')[0]

    page_num += 1

  for post_url in post_list:
    response_body = cache_request(
      'data_cache/%s.html' % post_url.replace('/', '_'),
      lambda: requests.get(post_url).text
    )

    soup = bs4.BeautifulSoup(response_body, 'html.parser')
    date_string = soup.select_one('span.date-display-single').attrs['content'].split('+')[0]
    date = datetime.datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')

    content = soup.select_one('div.field-name-body').text

    tmp_data = {}
    overseas = None
    community = None
    epi_link = None
    investigation = None

    regexes = {
      'recovered': [
        r'.*There are (?:now )?(?P<recovered>[\d,]+) (?:(?:reported cases)|(?:individuals)|(?:cases)|(?:people)|(?:people reported as)) (?:(?:of COVID-19 )?(?:with COVID-19 )?(?:infection )?(?:(?:(?:which )?(?:that )?we can confirm)|who) )?(?:have|are|having) recovered.*',
        r'.*total number of people who have recovered to (?P<recovered>[\d,]+)[^\d,].*',
        r'.*(?:(?:our cases,)|with|are|have) (?P<recovered>[\d,]+) (?:people )?(?:cases )?(?:that )?(?:are )?reported as (?:having )?recovered.*',
        r'.*We have (?P<recovered>[\d,]+) people who have recovered from COVID-19.*',
        r'.*as having recovered from COVID-19, an increase of \w+ on yesterday, for a total of (?P<recovered>[\d,]+)\..*',
        r'.*no change to the number of (?:people )?recovered (?:cases which remain )?at (?P<recovered>[\d,]+)[\. ].*',
        r'.*taking recoveries to (?P<recovered>[\d,]+)\..*',
        r'.*we can report \w+ new recovered cases taking the total to (?P<recovered>[\d,]+)\..*',
        r'.*recovered case(?:s)?(?: meaning this total)? is now (?P<recovered>[\d,]+)\..*',
        r'.*recovered cases is (?:unchanged at )?(?P<recovered>[\d,]+)\..*',
      ],
      'confirmed': [
        r'.*This means the current national total is (?P<confirmed>[\d,]+)[,\.].*',
        r'.*total (?:number )?of (?:confirmed and probable )?(?:COVID-19 )?cases (?:in New Zealand )?(is|to) (?:a total of )?(?P<confirmed>[\d,]+)[^\d,].*',
        r'.*total number of COVID-19 cases in New Zealand, which remains at (?P<confirmed>[\d,]+)[^\d,].*',
        r'.*total of confirmed and probable cases[^.]+ (to|at) (?P<confirmed>[\d,]+)[^\d,].*',
      ],
      'deaths': [
        r'.*the total of deaths in New Zealand to (?P<deaths>\d+)[^\d].*',
        r'.*New Zealand now has (?P<deaths>[^ ]+) (?:COVID-19 related )?deaths(?: associated with COVID-19)?.*',
        r'.*to report (a|(the country.s)) (?P<deaths>[^ ]+) death linked to COVID-19.*',
        r'.*There have now been (?P<deaths>[^ ]+) deaths from COVID-19.*',
        r'.*total number of confirmed COVID-19 deaths in New Zealand to (?P<deaths>[^.]+).*',
        r'.*we have one additional death to report today which takes our total to (?P<deaths>[^.]+).*',
        r'.*This is our (?P<deaths>[^ ]+) death from COVID-19.*'
      ],
      'hospitalized': [
        r'.*(?:(?:[Tt]here are)|(?:we have)|(?:can report)) (?P<hospitalized>[^ ]+) (?:people )?in hospital.*(((That|(The total)|(That total)) includes)|including) (?P<icu>[^ ]+) (?:people )?(?:person )?(?:in [^ ]+ )?in (?:the )?ICU[ \.].*',
        r'.*(?:(?:[Tt]here are)|(?:[Ww]e have)|(?:can report)) (?P<hospitalized>[^ ]+) people (?:remain )?in hospital(?: with COVID-19)?.*',
      ],
      'icu': [
        r'.*(?:(?:[Tt]here are)|(?:we have)|(?:can report)) (?P<hospitalized>[^ ]+) (?:people )?in hospital.*(((That|(The total)|(That total)) includes)|including) (?P<icu>[^ ]+) (?:people )?(?:person )?(?:in [^ ]+ )?in (?:the )?ICU[ \.].*',
        r'.*(?P<icu>([Nn]either)|([Nn]one)) (?:are )?in ICU.*'
      ],
      'tests': [
        r'.*total (?:(?:number of cases carried out)|(?:tests)|(?:(?:number )?of (lab )?tests)) (?:undertaken )?(?:completed )?to date (to|of|is|are) (?P<tests>[\d,]+)[^\d].*',
        r'.*[^\d,](?P<tests>[\d,]+) (?:total )?tests (?:have been )?processed to date\..*',
        r'.*tests completed(?: yesterday,)? (with|for) a combined total to date of (?P<tests>[\d,]+)\..*',
      ]
    }

    for group_name, regex_list in regexes.iteritems():
      for r in regex_list:
        m = re.match(r, content, re.MULTILINE | re.DOTALL)
        if m:
          matched = m.group(group_name)

          if matched.lower() in ['neither', 'none']:
            tmp_data[group_name] = 0
          elif matched.endswith('th') or matched in ['first', 'second', 'third']:
            tmp_data[group_name] = parse_ordinal(matched)
          else:
            tmp_data[group_name] = parse_num(matched)

          break

    m = re.match(r'.* to overseas travel \((?P<overseas>\d+)\%\).*links to confirmed cases within New Zealand \((?P<within_nz>\d+)\%\).*community transmission \((?P<community>\d+)\%\).*(?:still investigating (?P<investigation>\d+)\%)?.*', content, re.MULTILINE | re.DOTALL)
    if m:
      overseas_perc = parse_perc(m.group('overseas'))
      within_nz_perc = parse_perc(m.group('within_nz'))
      community_perc = parse_perc(m.group('community'))
      if m.group('investigation'):
        investigation_perc = parse_perc(m.group('investigation'))
      else:
        investigation_perc = None

      overseas = int(round(tmp_data['confirmed'] * overseas_perc))
      community = int(round(tmp_data['confirmed'] * community_perc))
      epi_link = int(round(tmp_data['confirmed'] * (within_nz_perc - community_perc)))
      if investigation_perc is not None:
        investigation = int(round(tmp_data['confirmed'] * investigation_perc))
    else:
      m = re.match(r'.* (?P<epi_link>\d+)\% involve contact with a confirmed case within New Zealand.*(?P<overseas>\d+)\% have a link with overseas travel.*community transmission accounts for (?P<community>\d+)\%.*still investigating (?P<investigation>\d+)\% of cases.*', content, re.MULTILINE | re.DOTALL)
      if m:
        overseas_perc = parse_perc(m.group('overseas'))
        epi_link_perc = parse_perc(m.group('epi_link'))
        community_perc = parse_perc(m.group('community'))
        investigation_perc = parse_perc(m.group('investigation'))

        overseas = int(round(tmp_data['confirmed'] * overseas_perc))
        community = int(round(tmp_data['confirmed'] * community_perc))
        epi_link = int(round(tmp_data['confirmed'] * epi_link_perc))
        investigation = int(round(tmp_data['confirmed'] * investigation_perc))


    if 'confirmed' in tmp_data:
      data[date.strftime('%Y-%m-%d')] = {
        'confirmed': tmp_data['confirmed'],
      }

      if 'recovered' in tmp_data:
        data[date.strftime('%Y-%m-%d')]['recovered'] = tmp_data['recovered']

      if 'deaths' in tmp_data:
        data[date.strftime('%Y-%m-%d')]['deaths'] = tmp_data['deaths']

      if 'hospitalized' in tmp_data:
        data[date.strftime('%Y-%m-%d')]['hospitalized'] = tmp_data['hospitalized']

      if 'icu' in tmp_data:
        data[date.strftime('%Y-%m-%d')]['icu'] = tmp_data['icu']

      if 'tests' in tmp_data:
        data[date.strftime('%Y-%m-%d')]['tested'] = tmp_data['tests']

      data[date.strftime('%Y-%m-%d')]['sources'] = {
        'Overseas acquired': overseas,
        'Locally acquired - contact of a confirmed case': epi_link,
        'Locally acquired - contact not identified': community,
        'Under investigation': investigation,
      }

  return data

def add_manual_data(timeseries_data):
  events = {
    # https://www.health.govt.nz/news-media/media-releases/single-case-covid-19-confirmed-new-zealand
    # https://www.health.govt.nz/news-media/media-releases/covid-19-3-march-2020-test-results-negative
    '2020-02-28': {
      'confirmed': 1,
      'deaths': 0,
      'recovered': 0,
      'hospitalized': 1,
      'icu': 0,
      'sources': {
        'Overseas acquired': 1,
        'Locally acquired - contact of a confirmed case': 0,
        'Locally acquired - contact not identified': 0,
        'Under investigation': 0,
      }
    },
    '2020-02-29': {},
    '2020-03-01': {},
    '2020-03-02': {},
    '2020-03-03': {},
    # https://www.health.govt.nz/news-media/media-releases/second-case-covid-19-confirmed-nz
    '2020-03-04': {
      'confirmed': 1,
      'sources': {
        'Overseas acquired': 1,
      }
    },
    # https://www.health.govt.nz/news-media/media-releases/third-case-covid-19-confirmed-new-zealand
    '2020-03-05': {
      'confirmed': 1,
      'sources': {
        'Overseas acquired': 1,
      }
    },
    # https://www.health.govt.nz/news-media/media-releases/fourth-case-covid-19-confirmed-new-zealand
    '2020-03-06': {
      'confirmed': 1,
      'sources': {
        'Overseas acquired': 1,
      }
    },
    # https://www.health.govt.nz/news-media/media-releases/fifth-case-covid-19-fits-pattern-previous-case
    '2020-03-07': {
      'confirmed': 1,
      'sources': {
        'Locally acquired - contact of a confirmed case': 1,
      }
    },
    '2020-03-08': {},
    '2020-03-09': {},
    '2020-03-10': {},
    '2020-03-11': {},
    '2020-03-12': {},
    '2020-03-13': {},
    # https://www.health.govt.nz/news-media/media-releases/covid-19-6th-case-overseas-link
    '2020-03-14': {
      'confirmed': 1,
      'sources': {
        'Overseas acquired': 1,
      }
    },
    # https://www.health.govt.nz/news-media/media-releases/covid-19-7th-and-8th-cases-overseas-links
    '2020-03-15': {
      'confirmed': 2,
      'sources': {
        'Overseas acquired': 2,
      }
    },
    '2020-03-16': {},
    # https://www.health.govt.nz/news-media/media-releases/covid-19-one-additional-case-linked-overseas-travel
    # https://www.health.govt.nz/news-media/media-releases/covid-19-three-new-cases-linked-overseas
    '2020-03-17': {
      'confirmed': 4,
      'sources': {
        'Locally acquired - contact of a confirmed case': 1,
        'Overseas acquired': 3,
      }
    },
    # https://www.health.govt.nz/news-media/media-releases/covid-19-eight-new-cases-linked-overseas-travel
    '2020-03-18': {
      'confirmed': 8,
      'sources': {
        'Overseas acquired': 8,
      }
    },
    # https://www.health.govt.nz/news-media/media-releases/new-cases-covid-19-confirmed-no-community-transmission
    '2020-03-19': {
      'confirmed': 8,
      'tested': 2300,
      'sources': {
        'Overseas acquired': 8,
      }
    },
    # https://www.health.govt.nz/news-media/media-releases/11-new-cases-covid-19
    '2020-03-20': {
      'confirmed': 11,
      'tested': 3300,
      'sources': {
        'Overseas acquired': 8,
      }
    },
    # https://www.health.govt.nz/news-media/media-releases/covid-19-update-21-march
    '2020-03-21': {
      'confirmed': 13 + 4, # We include probable going forwards
      'tested': 4800,
      'sources': {
        'Overseas acquired': 15,
        'Under investigation': 2,
      }
    },
    # https://www.health.govt.nz/news-media/media-releases/covid-19-update-22-march-2020
    '2020-03-22': {
      'confirmed': 14,
      'tested': 6000,
      'sources': {
        'Overseas acquired': 11,
        'Locally acquired - contact of a confirmed case': 1,
        'Locally acquired - contact not identified': 2, # Two from the day earlier
        'Under investigation': 2,
      }
    },
    # https://www.health.govt.nz/news-media/media-releases/36-new-cases-covid-19-new-zealand
    '2020-03-23': {
      'confirmed': 36,
      'tested': 7400,
    },
    # https://www.health.govt.nz/news-media/media-releases/40-new-confirmed-cases-covid-19-new-zealand
    '2020-03-24': {
      'confirmed': 40 + 3,
      'recovered': 12,
      'tested': 8300,
    },
    # https://www.health.govt.nz/news-media/media-releases/50-new-cases-covid-19-new-zealand
    '2020-03-25': {
      'confirmed': 50,
      'recovered': 10,
      'hospitalized': 6,
      'icu': 0,
      'tested': 9780,
    },
    # https://www.health.govt.nz/news-media/media-releases/78-new-cases-covid-19-new-zealand
    '2020-03-26': {
      'confirmed': 78,
      'recovered': 5,
      'hospitalized': 7,
      'icu': 0,
      'tested': 12683,
    },
    # https://www.health.govt.nz/news-media/media-releases/85-new-cases-covid-19-new-zealand
    '2020-03-27': {
      'confirmed': 85,
      'recovered': 10,
      'hospitalized': 8,
      'icu': 1,
      'tested': 12683 + 1479, # Average over the last week
    },
    # https://www.health.govt.nz/news-media/media-releases/83-new-cases-covid-19-new-zealand
    '2020-03-28': {
      'confirmed': 83,
      'recovered': 13,
      'hospitalized': 12,
      'icu': 2,
      'tested': 12683 + 1479 + 1613, # Average over the last week
    },
    # https://www.health.govt.nz/news-media/media-releases/sadly-first-death-covid-19-new-zealand
    '2020-03-29': {
      'confirmed': 63,
      'recovered': 6,
      'deaths': 1,
      'hospitalized': 9,
      'icu': 1,
      'tested': 12683 + 1479 + 1613 + 1786, # Average over the last week
    },
    # https://www.health.govt.nz/news-media/media-releases/76-new-confirmed-cases-covid-19
    '2020-03-30': {
      'confirmed': 75,
      'recovered': 6,
      'hospitalized': 12,
      'icu': 2,
      'tested': 12683 + 1479 + 1613 + 1786 + 1728, # Average over the last week
    },
    # https://www.health.govt.nz/news-media/media-releases/58-new-cases-covid-19
    '2020-03-31': {
      'confirmed': 58,
      'recovered': 11,
      'hospitalized': 14,
      'icu': 2,
      'tested': 12683 + 1479 + 1613 + 1786 + 1728 + 1777, # Average over the last week
    },
    # https://www.health.govt.nz/news-media/media-releases/61-new-cases-covid-19
    '2020-04-01': {
      'tested': 12683 + 1479 + 1613 + 1786 + 1728 + 1777 + 1843, # Average over the last week
    },
  }

  confirmed = 0
  deaths = 0
  recovered = 0
  sources = collections.defaultdict(lambda: 0)

  for date in sorted(events.keys()):
    event_data = events[date]

    if date not in timeseries_data:
      timeseries_data[date] = {}

    if 'confirmed' in event_data:
      confirmed += event_data['confirmed']
    timeseries_data[date]['confirmed'] = timeseries_data[date].get('confirmed', confirmed)

    if 'deaths' in event_data:
      deaths += event_data['deaths']
    timeseries_data[date]['deaths'] = timeseries_data[date].get('deaths', deaths)

    if 'recovered' in event_data:
      recovered += event_data['recovered']
    timeseries_data[date]['recovered'] = timeseries_data[date].get('recovered', recovered)

    if 'hospitalized' in event_data:
      timeseries_data[date]['hospitalized'] = timeseries_data[date].get('hospitalized', event_data['hospitalized'])
    else:
      timeseries_data[date]['hospitalized'] = timeseries_data[date].get('hospitalized', 0)

    if 'icu' in event_data:
      timeseries_data[date]['icu'] = timeseries_data[date].get('icu', event_data['icu'])
    else:
      timeseries_data[date]['icu'] = timeseries_data[date].get('icu', 0)

    if 'tested' in event_data:
      timeseries_data[date]['tested'] = timeseries_data[date].get('tested', event_data['tested'])

    for k, v in event_data.get('sources', {}).iteritems():
      sources[k] += v

    if 'sources' not in timeseries_data[date]:
      timeseries_data[date]['sources'] = copy.deepcopy(sources)

  absolute_overrides = {
    # https://www.health.govt.nz/news-media/media-releases/8-new-cases-covid-19
    '2020-04-17': {
      'deaths': 11,
    },
    # https://www.health.govt.nz/news-media/media-releases/9-new-cases-covid-19
    '2020-04-19': {
      'deaths': 12,
    },
    # https://www.health.govt.nz/news-media/media-releases/5-new-cases-covid-19-2
    '2020-04-21': {
      'deaths': 13,
    },
    # https://www.health.govt.nz/news-media/media-releases/3-new-cases-covid-19
    '2020-04-23': {
      'deaths': 15,
    },
    # https://www.health.govt.nz/news-media/media-releases/5-new-cases-covid-19
    '2020-04-24': {
      'deaths': 17,
    },
    # https://www.health.govt.nz/news-media/media-releases/5-new-cases-covid-19-1
    '2020-04-27': {
      'deaths': 19,
    },
    # https://www.health.govt.nz/news-media/media-releases/6-new-cases-covid-19-0
    '2020-05-02': {
      'deaths': 20,
    },
    # https://www.health.govt.nz/news-media/media-releases/two-new-cases-covid-19
    '2020-05-06': {
      'deaths': 21,
    },
  }

  for date, data in absolute_overrides.iteritems():
    for k, v in data.iteritems():
      timeseries_data[date][k] = v

  return timeseries_data

def fill_in_blanks(timeseries_data):
  deaths = 0
  hospitalized = 0
  icu = 0
  for date in sorted(timeseries_data.keys()):
    if timeseries_data[date].get('deaths', None) is None:
      timeseries_data[date]['deaths'] = deaths
    else:
      deaths = timeseries_data[date]['deaths']

    if timeseries_data[date].get('hospitalized', None) is None:
      timeseries_data[date]['hospitalized'] = hospitalized
    else:
      hospitalized = timeseries_data[date]['hospitalized']

    if timeseries_data[date].get('icu', None) is None:
      timeseries_data[date]['icu'] = icu
    else:
      icu = timeseries_data[date]['icu']

  return timeseries_data


def parse_num(num):
  if re.match(r'^[\d,]+$', num):
    return int(num.replace(',', ''))
  else:
    return w2n.word_to_num(num)

def parse_perc(perc):
  return float(perc.replace('%', '')) / 100.0

def parse_ordinal(ordinal):
  simple_nums = {
    'fifth': 5,
  }
  if ordinal in simple_nums:
    return simple_nums[ordinal]

  ordinal = ordinal.replace('first', 'one').replace('second', 'two').replace('third', 'three').replace('ieth', 'y')
  ordinal = re.sub(r'th$', '', ordinal)

  return parse_num(ordinal)

def munge_data_to_output(timeseries_data, dates, data_key):
  dates = sorted(timeseries_data.keys())
  values = [timeseries_data[d] for d in dates]

  # Generate a list of all keys for the given data series
  # There's probably a way to do this with a Python one liner, but I think this
  # is clearer
  keyset = set()
  for v in values:
    for k in v.get(data_key, {}).keys():
      keyset.add(k)
  keys = sorted(keyset)

  munged_data = {}
  for k in keys:
    munged_data[k] = []
    for d in dates:
      munged_data[k].append(timeseries_data[d].get(data_key, {}).get(k, 0))

  return {
    'keys': keys,
    'subseries': munged_data,
  }

def cache_request(cache_filename, request, force_cache=False):
  if os.path.exists(cache_filename) or force_cache:
    with open(cache_filename, 'rb') as f:
      return f.read()
  else:
    result = request()
    with open(cache_filename, 'wb') as f:
      f.write(result.encode('utf-8'))
    return result

if __name__ == '__main__':
  main()