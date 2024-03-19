from flask import Response, redirect as flask_redirect


def url_bool(url_parameter):
    url_parameter = url_parameter.lower()

    if url_parameter in ['true', '1']:
        return True
    elif url_parameter in ['false', '0']:
        return False
    else:
        return None


def create_file_path_response(file_path: str, redirect: bool):
    if redirect:
        return flask_redirect(f'/datasets/file/{file_path.removeprefix("/")}')
    else:
        response = Response(file_path)
        response.headers['Access-Control-Allow-Origin'] = '*'

        return response
