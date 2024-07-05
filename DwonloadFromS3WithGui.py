# to fix: an error occurred illegallocationconstraintexceptioncalling listobjectsv2constraints is incompatible for the region specific endpoint this request was sent to 

import os
import threading
import tkinter as tk
from tkinter import ttk, messagebox
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# AWS S3 Client
s3_client = boto3.client('s3')

class S3BrowserApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("S3 File Downloader")
        self.geometry(f"{int(self.winfo_screenwidth() * 0.75)}x600")

        self.original_items = []

        self.create_widgets()
        self.load_buckets()

    def create_widgets(self):
        self.bucket_frame = tk.Frame(self)
        self.bucket_frame.pack(pady=5, fill='x')

        self.bucket_label = tk.Label(self.bucket_frame, text="Selected Bucket:")
        self.bucket_label.pack(side='left', padx=(10, 5))

        self.bucket_combobox = ttk.Combobox(self.bucket_frame, state="readonly", width=60)
        self.bucket_combobox.pack(side='left', padx=(0, 10))
        self.bucket_combobox.bind("<<ComboboxSelected>>", self.on_bucket_selected)

        self.filter_entry = tk.Entry(self.bucket_frame, width=80)
        self.filter_entry.pack(side='left', padx=(5, 10))
        self.filter_entry.bind("<KeyRelease>", self.apply_filter)

        self.filter_button = tk.Button(self.bucket_frame, text="🗁", command=self.open_text_window)
        self.filter_button.pack(side='left', padx=3)

        self.clean_filter_button = tk.Button(self.bucket_frame, text="Clean filtering 🗑️", command=self.clear_filter)
        self.clean_filter_button.pack(side='left', pady=2)

        self.progress = ttk.Progressbar(self.bucket_frame, orient="horizontal", length=200, mode='determinate')
        self.progress.pack(side='right', padx=(5, 10))

        self.item_count_label = tk.Label(self.bucket_frame)
        self.item_count_label.pack(side='right')

        self.tree_frame = tk.Frame(self)
        self.tree_frame.pack(expand=True, fill="both", pady=10)

        self.tree_scroll = tk.Scrollbar(self.tree_frame)
        self.tree_scroll.pack(side="right", fill="y")

        self.tree = ttk.Treeview(self.tree_frame, columns=("Key", "Size", "Last Modified"), selectmode="extended", yscrollcommand=self.tree_scroll.set)
        self.tree.pack(expand=True, fill="both")
        self.tree_scroll.config(command=self.tree.yview)
        self.tree.bind("<<TreeviewSelect>>", lambda event: self.update_counter())
        self.tree.bind("<Control-c>", self.copy_selected_to_clipboard) 

        total_width = int(self.winfo_screenwidth() * 0.74)
        self.tree.column("Key", width=int(total_width * 0.8), anchor=tk.W)
        self.tree.column("Size", width=int(total_width * 0.1), anchor=tk.CENTER)
        self.tree.column("Last Modified", width=int(total_width * 0.1), anchor=tk.CENTER)

        self.tree.heading("Key", text="Key", anchor=tk.W, command=lambda: self.sort_column("Key", False))
        self.tree.heading("Size", text="Size", anchor=tk.CENTER, command=lambda: self.sort_column("Size", False))
        self.tree.heading("Last Modified", text="Last Modified", anchor=tk.CENTER, command=lambda: self.sort_column("Last Modified", False))
        self.tree['show'] = 'headings'

        self.selection_frame = tk.Frame(self)
        self.selection_frame.pack(fill='x', pady=(5,10))

        self.select_all_button = tk.Button(self.selection_frame, text="Select all", command=self.select_all)
        self.select_all_button.pack(side='top', pady=(0,5)) 

        self.deselect_all_button = tk.Button(self.selection_frame, text="Unselect all", command=self.deselect_all)
        self.deselect_all_button.pack(side='top')

        self.counter_label = tk.Label(self.selection_frame, text="Selected Items: 0, Total Size: 0 KB")
        self.counter_label.pack(fill='x', expand=True, pady=(20,5))

        self.download_button = tk.Button(self.selection_frame, text="Download", command=self.download_files_prompt)
        self.download_button.pack(pady=(0,10))
        self.disable_ui_components()

    def copy_selected_to_clipboard(self, event):
        selected_items = self.tree.selection()
        if not selected_items:
            return

        selected_values = [self.tree.item(item, 'values')[0] for item in selected_items]
        clipboard_content = "\n".join(selected_values)
        self.clipboard_clear()
        self.clipboard_append(clipboard_content)
        messagebox.showinfo("Success", f"Copied {len(selected_items)} keys to clipboard")
        
    def select_all(self):
        for item in self.tree.get_children():
            self.tree.selection_add(item)
        self.update_counter()

    def deselect_all(self):
        self.tree.selection_remove(self.tree.get_children())
        self.update_counter()

    def load_buckets(self):
        try:
            buckets = s3_client.list_buckets()
            bucket_names = [bucket['Name'] for bucket in buckets['Buckets']]
            self.bucket_combobox['values'] = bucket_names
        except (NoCredentialsError, PartialCredentialsError) as e:
            messagebox.showerror("AWS Credentials Error", str(e))
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def on_bucket_selected(self, event):
        selected_bucket = self.bucket_combobox.get()
        thread = threading.Thread(target=self.load_s3_objects, args=(selected_bucket,))
        thread.daemon = True
        thread.start()
        self.enable_ui_components()

    def load_s3_objects(self, bucket_name):
        try:
            self.tree.delete(*self.tree.get_children())
            self.original_items = []
            
            self.item_count_label['text'] = "Indexing items in the bucket..."
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix='')
            
            objects_processed = 0

            total_objects = sum(1 for page in pages for obj in page.get('Contents', [])
                            if not obj['Key'].endswith('/'))

            if total_objects == 0:
                messagebox.showinfo("No Items", "No items found in the selected bucket.")
                self.disable_ui_components()
                return

            pages = paginator.paginate(Bucket=bucket_name, Prefix='')

            self.update_progress(0, total_objects)

            for page in pages:
                for obj in page['Contents']:
                    if obj['Key'].endswith('/'):
                        continue

                    item = (obj['Key'], obj['Size'], obj['LastModified'])
                    self.original_items.append(item)
                    self.tree.insert('', 'end', values=item)
                    objects_processed += 1
                    self.update_progress(objects_processed, total_objects)
                    self.item_count_label['text'] = f"Items collected from the bucket: {len(self.original_items)} of {total_objects}"

        except Exception as e:
            messagebox.showerror("Error", str(e))
        finally:
            self.update_progress(0, total_objects)

    def disable_ui_components(self):
        self.select_all_button.configure(state='disabled')
        self.deselect_all_button.configure(state='disabled')
        self.filter_button.configure(state='disabled')
        self.clean_filter_button.configure(state='disabled')
        self.filter_entry.configure(state='disabled')
        self.download_button.configure(state='disabled')

    def enable_ui_components(self):
        self.select_all_button.configure(state='normal')
        self.deselect_all_button.configure(state='normal')
        self.filter_button.configure(state='normal')
        self.clean_filter_button.configure(state='normal')
        self.filter_entry.configure(state='normal')
        self.download_button.configure(state='normal')
            
    def update_progress(self, value, maximum):
        self.progress['maximum'] = maximum
        self.progress['value'] = value
        self.update_idletasks()

    def apply_filter(self, event):
        filter_text = self.filter_entry.get().lower().strip()
        if not filter_text:
            self.clear_filter()
        else:
            self.tree.delete(*self.tree.get_children())
            for item in self.original_items:
                if filter_text in item[0].lower():
                    self.tree.insert('', 'end', values=item)

    def clear_filter(self):
        self.filter_entry.delete(0, tk.END)
        self.tree.delete(*self.tree.get_children())
        for item in self.original_items:
            self.tree.insert('', 'end', values=item)

    def sort_column(self, col, reverse):
        l = [(self.tree.set(k, col), k) for k in self.tree.get_children('')]
        l.sort(reverse=reverse)
        for index, (val, k) in enumerate(l):
            self.tree.move(k, '', index)
        reverse = not reverse
        self.tree.heading(col, command=lambda _col=col, _reverse=reverse: self.sort_column(_col, _reverse))

    def open_text_window(self):
        self.new_window = tk.Toplevel(self)
        self.new_window.title("Input Text")
        label = tk.Label(self.new_window, text="Insert session IDs, divide them with a new line.")
        label.pack(pady=10)
        self.textbox = tk.Text(self.new_window, wrap='word')
        self.textbox.pack(expand=True, fill='both', padx=10, pady=10)
        apply_button = tk.Button(self.new_window, text="Apply", command=self.apply_text)
        apply_button.pack(pady=5)

    def apply_text(self):
        input_text = self.textbox.get("1.0", 'end-1c').strip()
        session_ids = input_text.split('\n')
        self.filter_by_session_ids(session_ids)
        self.new_window.destroy()

    def filter_by_session_ids(self, session_ids):
        session_ids = [sid.lower().strip() for sid in session_ids if sid.strip()]
        self.tree.delete(*self.tree.get_children())
        for item in self.original_items:
            if any(sid in item[0].lower() for sid in session_ids):
                self.tree.insert('', 'end', values=item)

    def update_counter(self):
        selected_items = self.tree.selection()
        total_size = sum([int(self.tree.set(item, "Size").split()[0]) for item in selected_items if self.tree.set(item, "Size")])
        size_str = f"{total_size} B"
        if total_size > 1000:
            total_size /= 1024
            size_str = f"{total_size:.2f} KB"
        if total_size > 1000:
            total_size /= 1024
            size_str = f"{total_size:.2f} MB"
        if total_size > 1000:
            total_size /= 1024
            size_str = f"{total_size:.2f} GB"
        if total_size > 1000:
            total_size /= 1024
            size_str = f"{total_size:.2f} TB"
        self.counter_label.config(text=f"Selected Items: {len(selected_items)}\nTotal Size: {size_str}")

    def download_files_prompt(self):
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showinfo("No Selection", "No files selected for download.")
            return
        
        if messagebox.askyesno("Confirm Download", f"Are you sure that you want to download {len(selected_items)} files?"):
            self.start_download(selected_items)

    def start_download(self, items):
        download_window = tk.Toplevel(self)
        download_window.title("Downloading Files")
        download_window.geometry("500x200")

        self.total_files_label = tk.Label(download_window, text="")
        self.total_files_label.pack(pady=(20, 0))

        self.total_progress = ttk.Progressbar(download_window, orient="horizontal", length=300, mode='determinate')
        self.total_progress.pack(pady=(10, 20))

        self.perform_download(items, download_window)

        self.ok_button = tk.Button(download_window, text="   OK   ", command=download_window.destroy)
        self.ok_button.pack()

        thread = threading.Thread(target=self.perform_download, args=(items, download_window))
        thread.daemon = True 
        thread.start()


    def perform_download(self, items, window):
        total_files = len(items)
        s3_bucket = self.bucket_combobox.get()
        directory = os.path.join(os.path.dirname(__file__), 'downloaded_from_S3', s3_bucket)
        os.makedirs(directory, exist_ok=True)

        self.total_progress['maximum'] = total_files

        for index, item_id in enumerate(items, start=1):
            item = self.tree.item(item_id)['values']
            s3_key = item[0]
            file_path = os.path.join(directory, os.path.basename(s3_key))

            self.total_files_label.config(text=f"Downloading file {os.path.basename(s3_key)} [ {index} of {total_files} ]")
            
            self.current_file_size = int(item[1])

            self.downloading_process(file_path, s3_bucket, s3_key)

            self.total_progress['value'] = index
            window.update_idletasks()

    def downloading_process(self, local_file_path, s3_bucket, s3_key):
        try:
            with open(local_file_path, 'wb') as f:
                    s3_client.download_fileobj(s3_bucket, s3_key, f)
            print(f"Download completed: {local_file_path}")
        except Exception as e:
            messagebox.showerror("Download Failed", f"Failed to download {s3_key}: {str(e)}")


if __name__ == "__main__":
    app = S3BrowserApp()
    app.mainloop()
