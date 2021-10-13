import logging
import apache_beam

class WriteToGCS(DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self,message):
        
        """Write messages in a batch to Google Cloud Storage.

        Parameters
        ----------
        message : Pcollection.
            The input message from Pub/Sub Topic

        Raises
        ------
        Exception
            Raise an exception in case of missing bucket.
        """
        try:
            logging.getLogger().setLevel(logging.INFO)
            logging.info(f"input message: {message}")
            filename = "-".join([self.output_path, str(datetime.datetime.now())])
            logging.info(f"output_path : {filename}")
            with io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
                logging.info(f"message to save: {message}")
                f.write(f"message: {message}".encode("utf-8"))
        except Exception as e:
            logging.info(f"Exception catched {e}")
            pass
