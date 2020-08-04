#!/usr/bin/env python3
# -*- coding: utf8 -*-
import re


def is_word_in_string(word, string):
    """
    Есть ли заданое слово в заданной строке?

    Входные параметры:
    word -- заданное слово
    string -- заданная строка
    """
    string = re.sub(r'[^\w\s]', '', string)
    for w in string.split():
        if w == word:
            return True
    return False


def find_locate_max(lst):
    """
    Поиск наибольшего значения в списке.

    Входные параметры:
    lst -- список с числами
    """
    biggest = max(lst)
    return biggest, [index for index, element in enumerate(lst) if biggest == element]
