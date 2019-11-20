"""
Scrapes web-sites containing information used for DFS.
"""
import datetime
import re
import time

from bs4 import BeautifulSoup
import dfs.db.qual_ctrl as dqc
import dfs.scrape.utils as scrape
import dfs.utils.main as dfm
import numpy as np
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

#####################################################################

name_converter = dqc.NameConverter(excl_teams=True)


def convert_name (name, print_warning=False):
    try:
        return name_converter.clean(name)
    except:
        if print_warning:
            print('Warning: convert_name() failed to convert {}.'.format(name))
        return name


#####################################################################

class DailyNBALineups(object):
    """Scrape starting lineups from dailynbalineups.com."""
    URL = 'https://dailynbalineups.com/'

    def __init__ (self):
        self.df = pd.DataFrame(columns=['name', 'team', 'pos', 'fds', 'dks'])
        self.team_conversion = dfm.create_team_mapping('mascot',
                                                       'nba_code')

    def scrape (self):
        soup = scrape.get_soup_from_url(self.URL)
        game_tags = soup.find('div', {'id':'sdns_today'}).find_all('div', {
            'class':'sdns_match'})
        for game_tag in game_tags:
            rows = self._extract_game_starters(game_tag)
            self.df = self.df.append(rows, ignore_index=True)
        self.df['team'] = self.df['team'].apply(self.convert_team)
        self.df['name'] = self.df['name'].apply(convert_name)
        if name_converter.some_names_missing:
            print('Warning: Failed to convert following names:')
            for name in name_converter.unhandled_names:
                print('\t{}'.format(name))
        return self.df

    def _extract_game_starters (self, tag):
        """Create DF from tag for single game.

        Notes:
            Assumes the away team's players are listed on the left and the home
            team's on the right.
        """
        away_team = tag.find('div', {'class':'sdns_team away_team'}).find(
              'span').text
        home_team = tag.find('div', {'class':'sdns_team home_team'}).find(
              'span').text
        team_tables = tag.find(
              'div', {'class':'sdns_match_wrapper'}).find_all(
              'table', {'class':'table-fill table-match'})
        away_rows = self._extract_team_starters(team_tables[0])
        for i, _ in enumerate(away_rows):
            away_rows[i]['team'] = away_team
        home_rows = self._extract_team_starters(team_tables[1])
        for i, _ in enumerate(home_rows):
            home_rows[i]['team'] = home_team
        return away_rows + home_rows

    def _extract_team_starters (self, team_table):
        """<table class="table-fill table-match">

        Notes:
            Assumes that the first salary listed is FanDuel's and the second
            DraftKing's.
        """
        rows = []
        cells = team_table.find('tbody').find_all('td')
        for td in cells:
            row = {'name':td.find('a', {'class':'sdns_player_data'}).text,
                   'pos':td.find('span', {'class':'pos'}).text}
            sal_tags = td.find_all('a', {'class':'sal'})
            try:
                row['fds'] = self.convert_salary(sal_tags[0].text)
            except:
                row['fds'] = np.NaN
            try:
                row['dks'] = self.convert_salary(sal_tags[1].text)
            except IndexError:
                row['dks'] = np.NaN
            rows.append(row)
        return rows

    @staticmethod
    def convert_salary (salary_str):
        """Converts salary string (e.g., '$8.7K') to number (8700)."""
        regexp = '\$(\d{1,2})\.(\d)K'
        pieces = re.findall(regexp, salary_str)[0]
        thousands, hundreds = int(pieces[0]), int(pieces[1])
        return int(thousands*1000 + hundreds*100)

    def convert_team (self, team_name):
        """Converts web-site team name (e.g., Lakers) to database code."""
        try:
            return self.team_conversion[team_name]
        except:
            print('Failed converting team name: {}'.format(team_name))
            return None


class RotoWorldInjuries(object):
    """Scrapes injury information from RotoWorld injuries page.

    Attributes:
        df (pd.DataFrame): Scraping results.
        team_conversion (dict): Maps teams' full names to the database code.
        driver: Web driver.
        year (int): Current year.
        month (int): Current month. Along with `year` attribute, is used when
        creating datetime objects to determine whether a date took place in a
        prior year.
    """

    URL = 'http://www.rotoworld.com/teams/injuries/nba/all/'

    COLS_ALL = ['start_date', 'player', 'team', 'status', 'report', 'inj',
                'returns', 'pid', 'report_date']
    COLS_STR = ['player', 'team', 'status', 'inj', 'returns', 'report']
    COLS_DATE = ['start_date', 'report_date']
    COLS_NUMERIC = ['pid']

    def __init__ (self):
        """Inits WebDriver and loads site HTML."""
        self.df = pd.DataFrame(columns=self.COLS_ALL)
        self.team_conversion = dfm.create_team_mapping('full_name', 'nba_code')
        today = datetime.datetime.now()
        self.year = today.year
        self.month = today.month
        self.driver = scrape.get_selenium_driver('chrome')
        self.driver.get(self.URL)
        time.sleep(2)

    def scrape (self):
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        team_rows = soup.find('div', {'id':'cp1_pnlInjuries'}).find_all(
              'div', {'class':'pb'})
        for team_row in team_rows:
            team = team_row.find('div', {'class':'headline'}).find('a').text
            team = self.convert_team(team)
            # Extract individual injury news rows (first row in the main
            # table contains column headers so we exclude it).
            inj_rows = team_row.find('tbody').find_all('tr')[1:]
            for inj_row in inj_rows:
                row = self.extract_info_from_inj_row(inj_row, team)
                self.df = self.df.append(row, ignore_index=True)
        self.driver.close()
        self.df['player'] = self.df['player'].apply(convert_name)
        if name_converter.some_names_missing:
            print('Warning: Failed to convert following names:')
            for name in name_converter.unhandled_names:
                print('\t{}'.format(name))
        return self.df

    def extract_info_from_inj_row (self, row, team):
        """Create row for DataFrame from injury HTML row."""
        r = {}
        cells = row.find_all('td')
        r['team'] = team
        r['player'] = cells[0].find('a').text
        inj_div = cells[1].find('div', {'class':'playercard'})
        r['pid'] = int(inj_div.attrs['id'])
        r['inj'] = inj_div.find('span').text
        r['report'] = inj_div.find('div', {'class':'report'}).text
        r['returns'] = inj_div.find('div', {'class':'impact'}).text
        r['report_date'] = self.parse_date(
              scrape.clean_html(inj_div.find('div', {'class':'date'}).text))
        r['status'] = cells[3].text
        r['start_date'] = self.parse_date(cells[4].text)
        return r

    def parse_date (self, date_str):
        """Converts Rotoworld date description (e.g., 'Jan 11') to datetime."""
        try:
            date_str = scrape.clean_html(date_str)
            regexp = '([A-Z][a-z]{2,3})\s?(\d{1,2})'
            pieces = re.findall(regexp, date_str)[0]
            month = scrape.month_conversion[pieces[0]]
            if month>self.month:
                year = self.year - 1
            else:
                year = self.year
            day = int(pieces[1])
            return datetime.datetime(year, month, day)
        except:
            print('Warning: Failed to clean following string to datetime: '
                  '{}.'.format(date_str))
            return None

    def convert_team (self, team_name):
        """Prints warning if unable to map RotoWorld team name to DB code."""
        try:
            return self.team_conversion[team_name]
        except:
            print('Warning: Failed converting team name: {}.'.format(team_name))
            return team_name


class RotoWorldNews(object):
    """Download news from RotoWorld.

    Attributes:
        n_pages_update (int): An update msg will be printed for every
        `n_pages_update` pages that are scraped.
    """
    URL = 'http://www.rotoworld.com/playernews/nba/basketball-player-news'
    COLS_ALL = ['player', 'player_link', 'team', 'team_link', 'report',
                'impact', 'date', 'source', 'source_link', 'related']
    # All columns containing strings (i.e.).
    COLS_STR = ['player', 'player_link', 'team', 'team_link', 'report',
                'impact', 'source', 'source_link', 'related']
    # Columns containing strings we need to inspect for sql-readiness.
    COLS_STR_SQL_CLEAN = ['player', 'report', 'impact', 'source',
                          'source_link', 'related']
    COLS_DATE = ['date']
    # Columns we want to run through an html string cleaner.
    COLS_STR_HTML_CLEAN = ['player', 'player_link', 'team_link', 'report',
                           'impact', 'source', 'source_link', 'related']

    def __init__ (self, sleep, early_thresh, max_pages):
        # Init DataFrame storing results.
        self.df = pd.DataFrame(columns=self.COLS_ALL)
        # Init other attrs.
        self.early_thresh = early_thresh
        self.max_pages = max_pages
        self.page_count = 0
        self.n_pages_update = 5
        self.team_conversion = dfm.create_team_mapping('mascot', 'nba_code')
        # Init web driver and load web-site.
        self.driver = scrape.get_selenium_driver('chrome')
        self.sleep = sleep
        self.driver.get(self.URL)
        time.sleep(self.sleep)

    def scrape (self):
        # Find news item tags.
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        news_tags = soup.find('div', {'id':'RW_main'}).find('div', {
            'class':'RW_playernews stretch'}).find_all('div', {'class':'pb'})
        # Create data row from each tag and add to DF.
        for tag in news_tags:
            r = self.create_news_row(tag)
            self.df = self.df.append(r, ignore_index=True)
        self.page_count += 1
        if self.page_count%self.n_pages_update==0:
            print('Finished page {}.'.format(self.page_count))

        ##################################################
        # Decide whether to terminate or run recursively.
        ##################################################
        terminate = False
        # Terminate if earliest date in scraped data occurs before
        # early_thresh.
        earliest_news = self.df['date'].min()
        if earliest_news<=self.early_thresh:
            self.df = self.df[self.df['date']>self.early_thresh]
            terminate = True
        # Terminate if we have exceeded max number of page loads.
        elif self.page_count>=self.max_pages:
            print('Warning: Exceeded max_pages, causing termination before '
                  'reaching early threshold.')
            terminate = True

        # Click button to view older news and call procedure recursively.
        if not terminate:
            older_btn = self.driver.find_element_by_id(
                  'cp1_ctl00_btnNavigate1Bot')
            older_btn.click()
            time.sleep(self.sleep)
            self.scrape()

        ##################################################
        # Finish procedure.
        ##################################################
        # Format columns that may contain messy HTML.
        for col in self.COLS_STR_HTML_CLEAN:
            self.df[col] = self.df[col].apply(scrape.clean_html, args=(False,))
        # Update names to be DB-friendly.
        self.df['player'] = self.df['player'].apply(convert_name)
        if name_converter.some_names_missing:
            print('Warning: Failed to convert following names:')
            for name in name_converter.unhandled_names:
                print('\t{}'.format(name))
        return self.df

    @staticmethod
    def _parse_related_players (tag):
        """Parses related players to news event from <div class="related">."""
        if tag is None:
            return None
        elif len(tag.text)==0:
            return None
        try:
            s = ''
            for i, a_tag in enumerate(tag.find_all('a')):
                name = convert_name(a_tag.text)
                link = a_tag.attrs['href']
                if i>0:
                    s += ';'
                s += '{}|{}'.format(name, link)
            return s
        except:
            return None

    def create_news_row (self, tag):
        """Creates row for player news item in <div class="pb"> tag."""
        r = {}
        # Name & team info.
        name_team_links = tag.find('div', {'class':'player'}).find_all('a')
        r['player_link'] = name_team_links[0].attrs['href']
        r['player'] = name_team_links[0].text
        r['team_link'] = name_team_links[1].attrs['href']
        r['team'] = self.convert_team(name_team_links[1].text)
        # News info.
        report_tag = tag.find('div', {'class':'report'})
        if report_tag is None:
            r['report'] = None
        else:
            r['report'] = report_tag.text
        impact_tag = tag.find('div', {'class':'impact'})
        if impact_tag is None:
            r['impact'] = None
        else:
            r['impact'] = impact_tag.text
        # Date.
        date_str = tag.find('div', {'class':'info'}).find(
              'div', {'class':'date'}).text
        r['date'] = self.parse_date(date_str)
        # Source info.
        source_tag = tag.find('div', {'class':'source'}).find('a')
        if source_tag is None:
            r['source'] = None
            r['source_link'] = None
        else:
            r['source'] = source_tag.text
            r['source_link'] = source_tag.attrs['href']
        # Related players.
        related_tag = tag.find('div', {'class':'info'}).find(
              'div', {'class':'related'})
        r['related'] = self._parse_related_players(related_tag)
        return r

    def convert_team (self, team_name):
        """Converts Rotoworld team name (e.g., Lakers) to database code."""
        try:
            return self.team_conversion[team_name]
        except:
            print('Failed converting team name: {}'.format(team_name))
            return None

    @staticmethod
    def parse_date (date_str):
        """Converts Rotoworld date description to datetime."""
        try:
            regexp = '([A-Z][a-z]{2,3})\s(\d{1,2})\s\-\s(\d{1,2}):(\d{2})\s(' \
                     'AM|PM)'
            pieces = re.findall(regexp, date_str)[0]
            month = scrape.month_conversion[pieces[0]]
            day, hour, minute = int(pieces[1]), int(pieces[2]), int(pieces[3])
            if pieces[4]=='PM':
                hour += 12
            return datetime.datetime(2018, month, day, hour, minute)
        except:
            return None


class OddsShark(object):
    """Scrape Total/Spread Vegas odds from oddsshark.com.

    Notes:
        The size of the browser affects whether team names are displayed in
        `short_name` version (e.g., Chicago, LA Lakers) or whether they
        appear in `nba_code` version (e.g., CHI, LAL).

        The resulting DataFrame has both a `team` and `opp` column because it
        cannot yet separate games taking place on a future day and so the
        `team`/`opp` combination can be used to ensure we reference the right
        row.
    """

    URL = 'http://www.oddsshark.com/nba/odds'

    def __init__ (self, sleep):
        self.df = pd.DataFrame(columns=['team', 'opp', 'total', 'spread'])
        self.sleep = sleep
        self.driver = scrape.get_selenium_driver('chrome')
        self.driver.get(self.URL)
        self.team_conversion = dfm.create_team_mapping(
              key='short_name', value='nba_code')
        time.sleep(self.sleep)

    def scrape (self):
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        # Extract teams.
        matchups = soup.find(
              'div', {'class':'op-left-column-wrapper'}).find_all(
              'div', {'class':'op-matchup-wrapper basketball'})
        matchup_teams = []
        for match in matchups:
            teams = self.extract_teams_from_match(match)
            matchup_teams.append(teams)

        # Extract spreads.
        odds_rows = soup.find(
              'div', {'id':'op-results'}).find_all(
              'div', {'class':'op-item-row-wrapper not-futures'})
        spreads = []
        for row in odds_rows:
            spreads.append(self.extract_spread(row))

        # Extract totals: 3 steps.
        # First, click button which makes options for different views visible.
        option_link = self.driver.find_element_by_css_selector(
              '#op-sticky-header-wrapper > div.op-customization-wrapper > div '
              '> a')
        option_link.click()
        # Second, click option to view Totals.
        self.driver.wait = WebDriverWait(self.driver, 5)
        selector = '#op-sticky-header-wrapper > div.op-customization-wrapper ' \
                   '> div > ul > li:nth-child(3) > a'
        totals_link = self.driver.wait.until(
              EC.visibility_of_element_located((By.CSS_SELECTOR, selector)))
        totals_link.click()
        # Third, extract content from page.
        time.sleep(self.sleep)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        data_rows = soup.find(
              'div', {'id':'op-results'}).find_all(
              'div', {'class':'op-item-row-wrapper not-futures'})
        impl_totals = []
        for row in data_rows:
            impl_totals.append(self.extract_total(row))

        # Compile all information into DataFrame.
        for i, (team, opp) in enumerate(matchup_teams):
            team_spread = spreads[i]
            if np.isnan(team_spread):
                opp_spread = np.NaN
            else:
                opp_spread = -team_spread

            impl_total = impl_totals[i]
            row1 = {'team':team, 'opp':opp, 'total':impl_total,
                    'spread':team_spread}
            row2 = {'team':opp, 'opp':team, 'total':impl_total,
                    'spread':opp_spread}
            self.df = self.df.append(row1, ignore_index=True)
            self.df = self.df.append(row2, ignore_index=True)

        if self.teams_provided_in_short_name(matchup_teams):
            self.df['team'] = self.df['team'].apply(self.convert_team)
            self.df['opp'] = self.df['opp'].apply(self.convert_team)
        self.driver.close()
        return self.df

    @staticmethod
    def extract_teams_from_match (m):
        top = m.find('div', {'class':'op-matchup-team op-matchup-text '
                                     'op-team-top'}).find('a').text
        bottom = m.find('div', {
            'class':'op-matchup-team op-matchup-text op-team-bottom'}).find(
              'a').text

        return top, bottom

    def convert_team (self, team):
        """Converts team from `short_name` to `nba_code` form."""
        try:
            return self.team_conversion[team]
        except:
            print('Warning: Failed to clean {} to nba_code form.'.format(
                  team))
            return team

    def teams_provided_in_short_name (self, teams):
        """Returns True if the team names are in `short_name` form (e.g.,
        Chicago, LA Lakers) vs. `nba_code` form.
        """
        total_teams = len(teams)*2
        count_short = 0
        for (team, opp) in teams:
            if team in self.team_conversion:
                count_short += 1
            if opp in self.team_conversion:
                count_short += 1
        return (count_short/total_teams)>0.5

    @staticmethod
    def extract_spread (row):
        """Returns top number."""
        try:
            s = row.find('div', {'class':'op-item op-spread border-bottom '
                                         'op-opening'}).text
            s = scrape.clean_html(s)
            return float(s)
        except:
            return np.NaN

    @staticmethod
    def extract_total (row):
        try:
            t = row.find('div', {'class':'op-item op-spread border-bottom '
                                         'op-opening'}).text
            t = scrape.clean_html(t)
            t = t.replace('o', '').replace('u', '').strip()
            return float(t)
        except:
            return np.NaN
