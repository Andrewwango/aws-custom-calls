import math


def avg(a):
    """Takes list and returns average of list"""
    return sum(a) / len(a)


def redact(b, e, text):
    """Redact one portion of text with # defined by its start and end

    Args:
            b (int): start index of PII to be redacted
            e (int): end index of PII to be redacted
            text (str): text to be redacted

    Returns:
            str: redacted text with #
    """
    return text[:b] + "#" * (e - b) + text[e:]


def words_to_text(words):
    """Convert list of words to string of text.
    Limitations: there are spaces before punctuation
    (does not affect comprehend)

    Args:
            words (list): list of strings

    Returns:
            str: words as string
    """
    return " ".join(words)


def items_to_words(items):
    """Converts transcribe "items" JSON to a list of the
    most confident words

    Args:
            items (list): list of item objects according to transcribe JSON output

    Returns:
            list: list of most confident words per item
    """
    return [str(item["alternatives"][0]["content"]) for item in items]


def split_items_into_chunks(items):
    """Splits comprehend items output into chunks of
    items lists based on rough time to break down
    comprehend analysis (e.g. sentiment). Chunks
    are split based on punctuation.
    The chunk time depends on use case.

    Args:
            items (list): list of items from transcribe JSON output

    Returns:
            list: list of strings where each string represents one chunk
            list: list of end times of chunks
    """

    def calculate_chunk_time(t):
        if 0 <= t < 300:
            return 30
        elif 300 <= t < 600:
            return 60
        else:
            return 120

    def slice_transcript(p, words):
        sections = []
        for pi in range(1, len(p)):
            sections += [words_to_text(words[p[pi - 1] + 1 : p[pi] + 1])]
        sections[0] = words_to_text([words[0], sections[0]])
        return sections

    def index_of_closest_val(arr, val):
        return arr.index(min(arr, key=lambda x: abs(x - val)))

    def calculate_chunks(
        punctuation_positions, punctuation_times, selected_punctuation_indices
    ):
        chunk_ends = [0] + [
            punctuation_positions[p] for p in selected_punctuation_indices
        ]
        chunk_times = [punctuation_times[p] for p in selected_punctuation_indices]
        return chunk_ends, chunk_times

    start_times = []
    end_times = []
    end_time = 0
    punctuation_positions = []
    words = items_to_words(items)
    for it, item in enumerate(items):
        if "start_time" in item and "end_time" in item:
            start_times += [float(item["start_time"])]
            end_times += [float(item["end_time"])]
            end_time = end_times[-1]
        else:
            start_times += [0]
            end_times += [0]
            punctuation_positions += [it]

    chunk_time = calculate_chunk_time(end_time)
    punctuation_end_times = [
        start_times[punctuation_positions[pi] + 1]
        for pi in range(len(punctuation_positions) - 1)
    ] + [end_time]
    chunk_ends, chunk_times = calculate_chunks(
        punctuation_positions,
        punctuation_end_times,
        [
            index_of_closest_val(punctuation_end_times, (c + 1) * chunk_time)
            for c in range(math.ceil(end_time / chunk_time))
        ],
    )
    sections = slice_transcript(chunk_ends, words)
    return sections, chunk_times


def calculate_talkover_time(list_of_items):
    """Calculate talkover statistics based on
    multi channel transcription

    Args:
            list_of_items (list): list of items lists where each list represents a channel

    Returns:
            int: total talkover time
            int: total amount of times talkover happened
    """

    def intersections(a, b):
        ranges = []
        i = j = 0
        while i < len(a) and j < len(b):
            a_left, a_right = a[i]
            b_left, b_right = b[j]
            if a_right < b_right:
                i += 1
            else:
                j += 1
            if a_right >= b_left and b_right >= a_left:
                end_pts = sorted([a_left, a_right, b_left, b_right])
                middle = [end_pts[1], end_pts[2]]
                ranges.append(middle)
        ri = 0
        while ri < len(ranges) - 1:
            if ranges[ri][1] == ranges[ri + 1][0]:
                ranges[ri : ri + 2] = [[ranges[ri][0], ranges[ri + 1][1]]]
            ri += 1
        return ranges

    list_of_intervals = []
    for items in list_of_items:
        list_of_intervals.append(
            [
                [float(item["start_time"]), float(item["end_time"])]
                if "start_time" in item and "end_time" in item
                else [0, 0]
                for item in items
            ]
        )

    interval_diffs = [
        interval[1] - interval[0]
        for interval in intersections(list_of_intervals[0], list_of_intervals[1])
    ]
    return sum(interval_diffs), sum([0 if diff == 0 else 1 for diff in interval_diffs])
