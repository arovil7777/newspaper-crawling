from app.templates.content_templates import CONTENT_TEMPLATES


def get_content_template(url):
    for template in CONTENT_TEMPLATES:
        if template["site"] in url:
            return template
    return None
