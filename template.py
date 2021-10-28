import pickle
import multiprocessing as mp

def process_func(file_list, queue):
    try:
        for file in file_list:
            #some_work_func - некоторая функция, производящая обработку файла
            workout_result = some_work_func(file)
            queue_token = {file: workout_result}
            queue.put(queue_token)
    except KeyboardInterrupt:
        pass
    finally:
        queue.put('DONE')
    
def queue_func(queue, file_manager, stream_count):
    closed_processes = 0
    items_saved = 0
    while True:
        try:
            if queue.is_empty():
                pass
            else:
                item = queue.get()
                if item == 'DONE':
                    closed_processes += 1
                else:
                    file_manager.result.update(item)
                    items_saved += 1
                    if items_saved == 10:
                        file_manager.save_pickle()
                        items_saved = 0
            if closed_processes == stream_count:
                break
        except KeyboardInterrupt:
            pass
    file_manager.save_pickle()

class FlileManager():
    def __init__(self, result_file, file_directory):
        self.result = dict()
        self.result_file = result_file
        self.file_directory = file_directory
        with open(result_file,'rb') as p:
            #Открываем pickle, если файл поврежден или не найден - будет создан новый
            try:
                self.result = pickle.load(p)
            except (EOFError, pickle.UnpicklingError, FileNotFoundError):
                self.result = dict()

    #Метод для сохранения результатов обработки в Pickle
    def save_pickle(self):
        with open(result_file, 'wb') as p:
            pickle.dump(self.result, p)

    #Сбор необработанных файлов
    def collect_files(self) -> list:
        files_to_process = []
        for root, subdir, files in os.walk(file_directory):
            for file in files:
                #Здесь также задается расширение обрабатываемых файлов
                if file not in self.result.keys() and file.endswith('.txt'):
                    files_to_process.append(os.path.join(root,file))
        return files_to_process

class ProcessManager():
    def __init__(self):
        self.process_list = []
        self.queue = mp.Queue()
        self.queue_process = None
    
    def create_process(self, func, file_list):
        try:
            process = mp.Process(target = func, args = (file_list, self.queue))
            process.start()
            self.process_list.append(process)
        except mp.ProcessError:
            print('Error during process creation')
    
    def create_queue_process(self, file_manager):
        try:
            self.queue_process = mp.Process(target = queue_func, args = (self.queue, file_manager, len(self.process_list)))
            self.queue_process.start()
            self.queue_process.join()
        except mp.ProcessError:
            for proc in self.process_list:
                proc.terminate()
        
if __name__ == '__main__':
    try:
        stream_count = 5 #количество потоков обработки
        files_dir = 'путь/к/директории'
        result_file = 'result.pkl' #Файл с результатами обработки
    
        file_manager = FileManager(result_file, files_dir)
        process_manager = ProcessManager()
        files_to_process = file_manager.collect_files()
    
        for i in range(stream_count):
            #Общий список файлов разбивается на равные части для всех потоков
            process_manager.create_stream(process_func, files_to_process[i:len(files_to_process):stream_count])
        process_manager.create_queue_process(file_manager)
        #Пока очередь не закончит заботу, программа не завершится
    except KeyboardInterrupt:
        print('Пользователь завершил работу досрочно')
    else:
        print('Программа завершила работу')
