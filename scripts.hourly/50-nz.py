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

      if 'COVID-19' in title_div.text or 'new cases' in title_div.text:
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

    recovered = None
    confirmed = None
    hospitalized = None
    icu = None
    overseas = None
    community = None
    epi_link = None
    investigation = None
    tests = None
    deaths = None

    m = re.match(r'.*There are (?:now )?(?P<recovered>\d+) (?:(?:reported cases)|(?:individuals)|(?:cases)|(?:people)) (?:of COVID-19 )?(?:with COVID-19 )?(?:infection )?(?:(?:(?:which )?(?:that )?we can confirm)|who) (?:have|are) recovered.*', content, re.MULTILINE | re.DOTALL)
    if m:
      recovered = parse_num(m.group('recovered'))

    m = re.match(r'.*total (?:number )?of (?:confirmed )?(?:and probable )?(?:COVID-19 )?cases (?:in New Zealand )?(is|to) (?:a total of )?(?P<confirmed>\d+)[^\d].*', content, re.MULTILINE | re.DOTALL)
    if m:
      confirmed = parse_num(m.group('confirmed'))

    m = re.match(r'.*the total of deaths in New Zealand to (?P<deaths>\d+)[^\d].*', content, re.MULTILINE | re.DOTALL)
    if m:
      deaths = parse_num(m.group('deaths'))
    else:
      m = re.match(r'.*New Zealand now has (?P<deaths>[^ ]+) deaths associated with COVID-19.*', content, re.MULTILINE | re.DOTALL)
      if m:
        deaths = parse_num(m.group('deaths'))
      else:
        m = re.match(r'.*to report (a|(the country.s)) (?P<deaths>[^ ]+) death linked to COVID-19.*', content, re.MULTILINE | re.DOTALL)
      if m:
        deaths = parse_ordinal(m.group('deaths'))

    m = re.match(r'.*(?:(?:there are)|(?:we have)|(?:can report)) (?P<hospitalized>[^ ]+) (?:people )?in hospital.*(((That|(The total)|(That total)) includes)|including) (?P<icu>[^ ]+) (?:people )?(?:person )?(?:in [^ ]+ )?in ICU[ \.].*', content, re.MULTILINE | re.DOTALL)
    if m:
      hospitalized = parse_num(m.group('hospitalized'))
      icu = parse_num(m.group('icu'))
    else:
      m = re.match(r'.*(?:(?:there are)|(?:[Ww]e have)|(?:can report)) (?P<hospitalized>[^ ]+) people (?:remain )?in hospital with COVID-19.*', content, re.MULTILINE | re.DOTALL)
      if m:
        hospitalized = parse_num(m.group('hospitalized'))

    m = re.match(r'.* to overseas travel \((?P<overseas>\d+)\%\).*links to confirmed cases within New Zealand \((?P<within_nz>\d+)\%\).*community transmission \((?P<community>\d+)\%\).*(?:still investigating (?P<investigation>\d+)\%)?.*', content, re.MULTILINE | re.DOTALL)
    if m:
      overseas_perc = parse_perc(m.group('overseas'))
      within_nz_perc = parse_perc(m.group('within_nz'))
      community_perc = parse_perc(m.group('community'))
      if m.group('investigation'):
        investigation_perc = parse_perc(m.group('investigation'))
      else:
        investigation_perc = None

      overseas = int(round(confirmed * overseas_perc))
      community = int(round(confirmed * community_perc))
      epi_link = int(round(confirmed * (within_nz_perc - community_perc)))
      if investigation_perc is not None:
        investigation = int(round(confirmed * investigation_perc))
    else:
      m = re.match(r'.* (?P<epi_link>\d+)\% involve contact with a confirmed case within New Zealand.*(?P<overseas>\d+)\% have a link with overseas travel.*community transmission accounts for (?P<community>\d+)\%.*still investigating (?P<investigation>\d+)\% of cases.*', content, re.MULTILINE | re.DOTALL)
      if m:
        overseas_perc = parse_perc(m.group('overseas'))
        epi_link_perc = parse_perc(m.group('epi_link'))
        community_perc = parse_perc(m.group('community'))
        investigation_perc = parse_perc(m.group('investigation'))

        overseas = int(round(confirmed * overseas_perc))
        community = int(round(confirmed * community_perc))
        epi_link = int(round(confirmed * epi_link_perc))
        investigation = int(round(confirmed * investigation_perc))

    m = re.match(r'.*total (?:(?:number of cases carried out)|(?:tests)|(?:of lab tests)) to date (to|of|is) (?P<tests>[\d,]+)[^\d].*', content, re.MULTILINE | re.DOTALL)
    if m:
      tests = parse_num(m.group('tests'))
    else:
      m = re.match(r'.*[^\d,](?P<tests>[\d,]+) (?:total )?tests (?:have been )?processed to date\..*', content, re.MULTILINE | re.DOTALL)
      if m:
        tests = parse_num(m.group('tests'))

    if confirmed is not None:
      data[date.strftime('%Y-%m-%d')] = {
        'confirmed': confirmed,
      }

      if recovered is not None:
        data[date.strftime('%Y-%m-%d')]['recovered'] = recovered

      if deaths is not None:
        data[date.strftime('%Y-%m-%d')]['deaths'] = deaths

      if hospitalized is not None:
        data[date.strftime('%Y-%m-%d')]['hospitalized'] = hospitalized

      if icu is not None:
        data[date.strftime('%Y-%m-%d')]['icu'] = icu

      if tests is not None:
        data[date.strftime('%Y-%m-%d')]['tested'] = tests

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