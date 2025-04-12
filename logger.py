import logging
import watchtower

def get_logger(name: str, log_group: str, log_stream: str) -> logging.Logger:
    """
    Creates and returns a logger that writes logs to both AWS CloudWatch and the console.
    
    Args:
        name (str): The name of the logger.
        log_group (str): The CloudWatch log group name.
        log_stream (str): The CloudWatch log stream name.
    
    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        cw_handler = watchtower.CloudWatchLogHandler(
            log_group=log_group,
            stream_name=log_stream
        )
        cw_handler.setLevel(logging.INFO)
        logger.addHandler(cw_handler)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger
