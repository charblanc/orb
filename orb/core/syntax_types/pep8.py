import projex.text
from ..syntax import Syntax


class PEP8Syntax(Syntax):
    def display(self, name):
        return projex.text.pretty(name)

    def field(self, name, reference=False):
        base = projex.text.underscore(name)
        if reference:
            base += '_id'
        return base

    def getter(self, name):
        return 'get_' + projex.text.underscore(name)

    def schemadb(self, name):
        return projex.text.pluralize(projex.text.underscore(name))

    def setter(self, name):
        return 'set_' + projex.text.underscore(name)


Syntax.registerAddon('pep8', PEP8Syntax)