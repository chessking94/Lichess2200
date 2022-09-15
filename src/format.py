def format_result(game_text, tag):
    tag_text = game_text.headers.get(tag)
    # res_len = 3
    res = None

    if tag_text is not None:
        if tag_text == '1-0':
            res = '1.0'
        elif tag_text == '0-1':
            res = '0.0'
        elif tag_text == '1/2-1/2':
            res = '0.5'

    return res


def format_source_id(game_text, tag):
    tag_text = game_text.headers.get(tag)
    site_id = tag_text.split('/')[-1] if tag_text is not None else None

    return site_id
