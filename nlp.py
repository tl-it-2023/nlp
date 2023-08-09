from natasha import (
    Segmenter,
    LOC,
    MorphVocab,
    NamesExtractor, AddrExtractor
)
from yargy import (
    Parser,
    rule, and_, or_, not_
)
from yargy.interpretation import fact
from yargy.predicates import (
    eq, in_,
    type, normalized,
    dictionary,
    gte, lte, gram
)
from yargy.pipelines import morph_pipeline, pipeline
from yargy.tokenizer import MorphTokenizer, EOL

from pydantic import BaseModel
from typing import List
import os
from datetime import datetime

from read_text import read_files
from config import STORAGE_DIR


class Person(BaseModel):
    fio: str = None
    age: int = None
    date_of_birth: str = None
    gender: str = None
    citizenship: str = None
    residential_address: str = None
    salary: str = None
    profession: str = None
    experience: str = None
    employment: str = None
    schedule: List[str] = None
    education: str = None
    languages: List[str] = None
    phone: str = None
    email: str = None


def ner(data: str):
    global gender, age, date, employment, schedule, education, salary, experience, fio, profession, phone

    INT = type('INT')
    COMMA = eq(',')
    COLON = eq(':')
    DASH = in_('-—')
    DOT = eq('.')

    """**********************ФИО**********************"""

    morph_vocab = MorphVocab()
    names_extractor = NamesExtractor(morph_vocab)

    for match in names_extractor(data):
        facts = match.fact
        if all((facts.first, facts.last, facts.middle)):
            fio = ' '.join([facts.last, facts.first, facts.middle])

    """**********************Телефон**********************"""

    Call = morph_pipeline([
        'телефон',
        'тел.'
    ])

    plus = eq('+')
    scob = in_('()')
    tr = eq('-')
    ddot = or_(eq(':'), eq(':'))

    Tel = rule(
        rule(plus).optional(),
        rule(INT),
        rule(scob).optional(),
        rule(INT).optional(),
        rule(scob).optional(),
        rule(INT).optional(),
        rule(tr).optional(),
        rule(INT).optional(),
        rule(tr).optional(),
        rule(INT).optional()
    )

    TELEPHONE = rule(
        Call,
        rule(ddot).optional(),
        Tel
    )
    parser = Parser(TELEPHONE)
    for match in parser.findall(data):
        start, stop = match.span
        phone = data[start:stop]

    """**********************Почта**********************"""

    email = '' .join([x.strip(',.') for x in data.split() if '@' in x])

    """**********************Пол**********************"""

    GENDERS = {
        'Женщина': 'женщина',
        'Жен.': 'женщина',
        'Жен': 'женщина',
        'Мужчина': 'мужчина',
        'Муж.': 'мужчина',
        'Муж': 'мужчина'
    }
    GENDER = rule(in_(GENDERS))

    parser = Parser(GENDER)
    for match in parser.findall(data):
        start, stop = match.span
        gender = data[start:stop]

    """**********************Дата рождения**********************"""

    MONTHS = {
        'января': '01',
        'февраля': '02',
        'марта': '03',
        'апреля': '04',
        'мая': '05',
        'июня': '06',
        'июля': '07',
        'августа': '08',
        'сентября': '09',
        'октября': '10',
        'ноября': '11',
        'декабря': '12'
    }
    MONTH_NAME = dictionary(
        MONTHS
    )
    DAY = and_(
        gte(1),
        lte(31)
    )
    YEAR = and_(
        gte(1900),
        lte(2100)
    )
    DATE = rule(
        DAY,
        MONTH_NAME,
        YEAR
    )
    BIRTH = rule(
        morph_pipeline(['родиться', 'дата рождения', 'дата рождения:']),
        DATE
    )

    parser = Parser(BIRTH)
    for match in parser.findall(data):
        start, stop = match.span
        date = data[start:stop]

    try:
        date = date.split()[1:]
        date[1] = MONTHS.get(date[1])
    except Exception as _ex:
        print(_ex)

    """**********************Возраст**********************"""

    day, month, year = map(int, date)
    today = datetime.today()
    age = today.year - year - ((today.month, today.day) < (month, day))

    """**********************Занятость**********************"""

    TITLE = morph_pipeline(['Занятость:', 'Занятость'])
    TYPES = {
        'полная': 'full',
        'полная занятость': 'full',
        'частичная': 'part',
        'частичная занятость': 'part',
        'волонтерство': 'volunteer',
        'стажировка': 'intern',
        'проектная работа': 'project'
    }
    TYPE = morph_pipeline(TYPES)

    TYPES = rule(
        TYPE,
        rule(
            COMMA,
            TYPE
        ).optional().repeatable()
    )
    EMPLOYMENT = rule(
        TITLE,
        TYPES
    )
    REVERSE_EMPLOYMENT = rule(
        TYPES, TITLE
    )

    parser = Parser(or_(EMPLOYMENT, REVERSE_EMPLOYMENT))
    for match in parser.findall(data):
        start, stop = match.span
        employment = data[start:stop]

    """**********************График работы**********************"""

    TITLE = morph_pipeline(['График работы:', 'График работы'])
    TYPES = {
        'полный день': 'full',
        'сменный график': 'part',
        'вахтовый метод': 'vahta',
        'гибкий график': 'flex',
        'удаленная работа': 'remote',
        'стажировка': 'intern'
    }
    TYPE = morph_pipeline(TYPES)
    TYPES = rule(
        TYPE,
        rule(
            COMMA,
            TYPE
        ).optional().repeatable()
    )
    SCHEDULE = rule(
        TITLE,
        TYPES
    )

    parser = Parser(SCHEDULE)
    for match in parser.findall(data):
        start, stop = match.span
        schedule = data[start:stop]

    """**********************Образование**********************"""

    TITLE = morph_pipeline(['Образование:', 'Образование'])
    TYPES = {
        'основное общее': 'basic general',
        'среднее общее': 'average general',
        'среднее': 'average',
        'среднее профессиональное': 'secondary vocational',
        'бакалавриат': 'bachelor',
        'бакалавр': 'bachelor',
        'специалитет': 'specialty',
        'магистратура': 'magistracy',
        'высшее': 'higher'
    }
    TYPE = morph_pipeline(TYPES)
    TYPES = rule(
        TYPE,
        rule(
            COMMA,
            TYPE
        ).optional().repeatable()
    )
    EDUCATION = rule(
        TITLE, TYPES
    )
    REVERSE_EDUCATION = rule(
        TYPES, TITLE
    )

    parser = Parser(or_(EDUCATION, REVERSE_EDUCATION))
    for match in parser.findall(data):
        start, stop = match.span
        education = data[start:stop]

    """**********************Знание языков**********************"""

    TITLE = morph_pipeline(['Знание языков:', 'Знание языков'])
    TYPES_LANGUAGES = {
        'русский': 'russian',
        'китайский': 'chinese',
        'английский': 'english',
        'французский': 'french',
        'немецкий': 'german'
    }
    TYPE = morph_pipeline(TYPES_LANGUAGES)
    LEVELS = {
        'родной': 'native',
        'базовые знания': 'base',
        'могу проходить интервью': 'interview'
    }
    LEVEL = morph_pipeline(LEVELS)
    ONE_LANGUAGE = rule(TYPE, DASH.optional(), LEVEL.optional())
    TYPES = rule(
        ONE_LANGUAGE,
        rule(
            COMMA.optional(),
            ONE_LANGUAGE
        ).optional().repeatable()
    )
    LANGUAGES = rule(
        TITLE, TYPES
    )

    parser = Parser(LANGUAGES)
    languages = []
    for match in parser.findall(data):
        for token in match.tokens:
            if token.value.lower() in TYPES_LANGUAGES.keys():
                languages.append(token.value)

    """**********************Должность**********************"""

    def load_lines(path):
        with open(path, encoding='utf-8') as file:
            for line in file:
                yield line.rstrip('\n')

    SPECIALIZATIONS = set(load_lines(os.path.join(STORAGE_DIR, 'specialization.txt')))
    SUBSPECIALIZATIONS = set(load_lines(os.path.join(STORAGE_DIR, 'subspecialization.txt')))

    TITLE = morph_pipeline([
        'Желаемая должность и зарплата',
        'Желаемая должность: ',
        'Должность',
        'Работать',
        'сфера'
    ])

    DOT = eq('•')

    SUBTITLE = not_(DOT).repeatable()

    SPECIALIZATION = pipeline(SPECIALIZATIONS)

    SUBSPECIALIZATION = pipeline(SUBSPECIALIZATIONS)

    ITEM = rule(
        rule(DOT).optional(),
        or_(
            SPECIALIZATION,
            SUBSPECIALIZATION
        )
    )

    POSITION = rule(
        TITLE,
        rule(SUBTITLE).optional(),
        ITEM.repeatable()
    )

    TOKENIZER = MorphTokenizer().remove_types(EOL)

    parser = Parser(POSITION, tokenizer=TOKENIZER)
    for match in parser.findall(data):
        start, stop = match.span
        profession = data[start:stop]

    """**********************Желаемая зарплата**********************"""

    Money = fact(
        'Money',
        ['amount', 'currency'],
    )

    CURRENCIES = {
        'руб.': 'RUB',
        'грн.': 'GRN',
        'бел. руб.': 'BEL',
        'RUB': 'RUB',
        'EUR': 'EUR',
        'KZT': 'KZT',
        'USD': 'USD',
        'KGS': 'KGS',
        'рублей': 'RUB'
    }

    CURRENCIE = {
        'тыс.': 'тысяча',
        'млн.': 'миллион'
    }

    CURRENCY = pipeline(CURRENCIES).interpretation(
        Money.currency.normalized().custom(CURRENCIES.get)
    )

    R_1 = rule(gram('ADJF'),
               dictionary({'оплата', 'зарплата', 'оклад', 'доход'}))

    CURRENCy = pipeline(CURRENCIE).interpretation(
        Money.currency.normalized().custom(CURRENCIE.get)
    )

    def normalize_amount(value):
        return int(value.replace(' ', ''))

    AMOUNT = or_(
        rule(INT),
        rule(INT, INT),
        rule(INT, INT, INT),
    ).interpretation(
        Money.amount.custom(normalize_amount)
    )

    MONEY = rule(
        AMOUNT,
        CURRENCy.optional(),
        CURRENCY,
    )

    parser = Parser(or_(MONEY, R_1))
    for match in parser.findall(data):
        start, stop = match.span
        salary = data[start:stop]

    """**********************Опыт работы**********************"""

    TITLE = rule(morph_pipeline(['Опыт работы']), DASH)
    YEAR = rule(INT, normalized('год').optional())
    MONTH = rule(INT, normalized('месяц'))
    EXPERIENCE = rule(TITLE, YEAR, MONTH.optional())

    parser = Parser(EXPERIENCE)
    for match in parser.findall(data):
        start, stop = match.span
        experience = data[start:stop]

    """**********************Адрес проживания**********************"""
    import string
    segmenter = Segmenter()
    morph_vocab = MorphVocab()
    addr_extractor = AddrExtractor(morph_vocab)
    R = ()
    matches = addr_extractor(data)
    facts = [i.fact.as_json for i in matches]
    for i in range(len(facts)):
        tmp = list(facts[i].values())
    R = R + (tmp[1],':', tmp[0])
    vowels_str = " ".join(R)
    R = vowels_str

    (str.maketrans('', '', string.punctuation))
    print(R)

    """**********************Общий вывод**********************"""

    person = Person()
    try:
        person.fio = fio
    except Exception as _ex:
        print(_ex)
    try:
        person.gender = GENDERS.get(gender)
    except Exception as _ex:
        print(_ex)
    try:
        person.date_of_birth = '.'.join(date)
    except Exception as _ex:
        print(_ex)
    try:
        person.age = age
    except Exception as _ex:
        print(_ex)
    try:
        person.employment = ' '.join(employment.split()[1:])
    except Exception as _ex:
        print(_ex)
    try:
        person.schedule = ' '.join(schedule.split()[2:]).split(', ')
    except Exception as _ex:
        print(_ex)
    try:
        person.education = ' '.join(education.split()[1:])
    except Exception as _ex:
        print(_ex)
    try:
        person.languages = languages
    except Exception as _ex:
        print(_ex)
    try:
        person.salary = salary.replace(' ', '')
    except Exception as _ex:
        print(_ex)
    try:
        experience = experience.lower().replace('опыт работы', '')
        person.experience = ' '.join(experience.split()[1:])
    except Exception as _ex:
        print(_ex)
    try:
        profession = profession.lower().replace('желаемая должность и зарплата', '').replace(' •', ',').replace(' ,', ',')
        person.profession = ' '.join(profession.split()).strip()
    except Exception as _ex:
        print(_ex)
    try:
        phone = phone.lower().replace(': ', '').replace('телефон', '')
        person.phone = phone
    except Exception as _ex:
        print(_ex)
    try:
        person.email = email
    except Exception as _ex:
        print(_ex)

    return person


def nlp(filename):
    try:
        text = read_files(filename=filename)
    except FileNotFoundError:
        return 'FileNotFoundError'
    else:
        person_model = ner(text)
        return person_model


if __name__ == '__main__':
    print("Test git")
    print(nlp('resume.txt'))
