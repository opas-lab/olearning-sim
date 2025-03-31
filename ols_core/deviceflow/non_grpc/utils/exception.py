class InvalidStrategyNameException(Exception):
    """Exception raised for invalid strategy names."""

    def __init__(self, strategy_name: str, message: str = "Invalid strategy name",
                 original_exception: Exception = None):
        """
        Initialize the exception.

        Args:
            strategy_name (str): The invalid strategy name that caused the exception.
            message (str, optional): An explanation of the error. Defaults to "Invalid strategy name".
        """
        self.strategy_name = strategy_name
        self.message = f"{message}: {strategy_name}"
        self.original_exception = original_exception
        super().__init__(self.message)

    def __str__(self):
        """
        Return the string representation of the exception.

        Returns:
            str: A string representation of the error message.
        """
        return f"{self.__class__.__name__}({self.message!r})"

    def __repr__(self):
        """
       Return the official string representation of the exception.

       Returns:
           str: A detailed representation including the exception type.
       """
        return f"{self.__class__.__name__}" \
               f"({self.message!r}, original_exception={self.original_exception!r})"


class AddShelfException(Exception):
    """Exception raised when adding a shelf fails."""

    def __init__(self, shelf_id: str, message: str = "Failed to add shelf", original_exception: Exception = None):
        self.shelf_id = shelf_id
        self.message = f"{message} for shelf ID: {shelf_id}"
        self.original_exception = original_exception
        super().__init__(self.message)

    def __str__(self):
        """
        Return the string representation of the exception.

        Returns:
            str: A string representation of the error message.
        """
        return f"{self.__class__.__name__}({self.message!r})"

    def __repr__(self):
        """
       Return the official string representation of the exception.

       Returns:
           str: A detailed representation including the exception type.
       """
        return f"{self.__class__.__name__}" \
               f"({self.shelf_id!r}, {self.message!r}, original_exception={self.original_exception!r})"


class NotifyDispatchException(Exception):
    """Exception raised when _notify_dispatch fails."""

    def __init__(self, flow_id: str, message: str = "Failed to notify dispatch", original_exception: Exception = None):
        self.flow_id = flow_id
        self.message = f"{message} for Flow ID: {flow_id}"
        self.original_exception = original_exception
        super().__init__(self.message)

    def __str__(self):
        """
        Return the string representation of the exception.

        Returns:
            str: A string representation of the error message.
        """
        original_msg = f"\nOriginal exception: {self.original_exception}" if self.original_exception else ""
        return f"{self.message}{original_msg}"

    def __repr__(self):
        """
       Return the official string representation of the exception.

       Returns:
           str: A detailed representation including the exception type.
       """
        return f"{self.__class__.__name__}" \
               f"({self.flow_id!r}, {self.message!r}, original_exception={self.original_exception!r})"


class ShelfNotExistException(Exception):
    """Exception raised when the shelf for a flow does not exist."""

    def __init__(self, flow_id: str, message="Shelf does not exist for flow", original_exception: Exception = None):
        self.flow_id = flow_id
        self.message = f"{message} for flow ID: {flow_id}"
        self.original_exception = original_exception
        super().__init__(self.message)

    def __str__(self):
        """
        Return the string representation of the exception.

        Returns:
            str: A string representation of the error message.
        """
        original_msg = f"\nOriginal exception: {self.original_exception}" if self.original_exception else ""
        return f"{self.message}{original_msg}"

    def __repr__(self):
        """
       Return the official string representation of the exception.

       Returns:
           str: A detailed representation including the exception type.
       """
        return f"{self.__class__.__name__}" \
               f"({self.flow_id!r}, {self.message!r}, original_exception={self.original_exception!r})"


class MarkNotifyStartException(Exception):
    """Exception raised when marking notify_start fails."""

    def __init__(self, flow_id: str, compute_resource: str, message: str = "Failed to mark notify_start",
                 original_exception: Exception = None):
        self.flow_id = flow_id
        self.compute_resource = compute_resource
        self.message = f"{message} for flow ID: {flow_id} and compute resource: {compute_resource}"
        self.original_exception = original_exception
        super().__init__(self.message)

    def __str__(self):
        """
        Return the string representation of the exception.

        Returns:
            str: A string representation of the error message.
        """
        original_msg = f"\nOriginal exception: {self.original_exception}" if self.original_exception else ""
        return f"{self.message}{original_msg}"

    def __repr__(self):
        """
       Return the official string representation of the exception.

       Returns:
           str: A detailed representation including the exception type.
       """
        return f"{self.__class__.__name__}" \
               f"({self.flow_id!r}, {self.message!r}, original_exception={self.original_exception!r})"


class MarkNotifyCompleteException(Exception):
    def __init__(self, flow_id: str, compute_resource: str, message: str = "Failed to mark notify_complete",
                 original_exception: Exception = None):
        self.flow_id = flow_id
        self.compute_resource = compute_resource
        self.message = f"{message} for flow ID: {flow_id} and compute resource: {compute_resource}"
        self.original_exception = original_exception
        super().__init__(self.message)

    def __str__(self):
        """
        Return the string representation of the exception.

        Returns:
            str: A string representation of the error message.
        """
        original_msg = f"\nOriginal exception: {self.original_exception}" if self.original_exception else ""
        return f"{self.message}{original_msg}"

    def __repr__(self):
        """
       Return the official string representation of the exception.

       Returns:
           str: A detailed representation including the exception type.
       """
        return f"{self.__class__.__name__}" \
               f"({self.flow_id!r}, {self.message!r}, original_exception={self.original_exception!r})"

class GetConsumerException(Exception):
    def __init__(self, message="Failed to get Pulsar consumer due to invalid configuration",  original_exception: Exception = None):
        self.message = message
        self.original_exception = original_exception
        super().__init__(self.message)

    def __str__(self):
        """
        Return the string representation of the exception.

        Returns:
            str: A string representation of the error message.
        """
        original_msg = f"\nOriginal exception: {self.original_exception}" if self.original_exception else ""
        return f"{self.message}{original_msg}"

    def __repr__(self):
        """
       Return the official string representation of the exception.

       Returns:
           str: A detailed representation including the exception type.
       """
        return f"{self.__class__.__name__}" \
               f"({self.message!r}, original_exception={self.original_exception!r})"

