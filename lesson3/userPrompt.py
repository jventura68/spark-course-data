def prompt(question: str) -> bool:
    """ Prompt the yes/no-*question* to the user. """
    from distutils.util import strtobool

    while True:
        user_input = input(question + " [y/n]: ").lower()
        try:
            result = strtobool(user_input)
            return result
        except ValueError:
            print("Please use y/n or yes/no.\n")

