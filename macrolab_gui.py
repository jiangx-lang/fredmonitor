"""
MacroLab GUI界面

提供图形化界面来运行MacroLab系统。
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import threading
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 加载环境变量
load_dotenv('macrolab.env')

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.fred_client import FredClient
from core.cache import CacheManager
from core.registry import FactorRegistry
from core.aggregator import DataAggregator
from core.report import ReportGenerator
from core.utils import load_yaml_config
from core.user_config import get_user_config, should_auto_confirm, get_user_id, log_user_action


class MacroLabGUI:
    """MacroLab图形界面"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("FRED 风险监控系统 - 宏观危机监测 + 日度风险面板")
        self.root.geometry("1000x700")
        self.root.configure(bg='#f0f0f0')
        
        # 初始化用户配置
        self.user_config = get_user_config()
        self.user_id = get_user_id()
        self.auto_confirm = should_auto_confirm()
        
        # 初始化变量
        self.fred_client = None
        self.cache_manager = None
        self.aggregator = None
        self.report_generator = None
        self.is_running = False
        
        # 记录用户启动
        log_user_action("启动MacroLab GUI", f"用户ID: {self.user_id}, 自动确认: {self.auto_confirm}")
        
        # 创建界面
        self.create_widgets()
        self.initialize_system()
    
    def create_widgets(self):
        """创建界面组件"""
        # 主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(2, weight=1)
        
        # 标题
        title_label = ttk.Label(main_frame, text="🚨 FRED 风险监控系统", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        
        # 副标题
        subtitle_label = ttk.Label(main_frame, text="宏观危机监测 + 日度风险面板", 
                                 font=('Arial', 12))
        subtitle_label.grid(row=0, column=0, columnspan=3, pady=(25, 10))
        
        # 左侧控制面板
        control_frame = ttk.LabelFrame(main_frame, text="控制面板", padding="10")
        control_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        
        # 系统状态
        status_frame = ttk.LabelFrame(control_frame, text="系统状态", padding="5")
        status_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.status_label = ttk.Label(status_frame, text="正在初始化...", foreground="orange")
        self.status_label.pack()
        
        # 功能按钮
        button_frame = ttk.LabelFrame(control_frame, text="功能操作", padding="5")
        button_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 数据管理按钮 - 放在最前面
        self.download_data_btn = ttk.Button(button_frame, text="📥 下载FRED数据", 
                                          command=self.download_fred_data)
        self.download_data_btn.pack(fill=tk.X, pady=2)
        
        self.check_data_btn = ttk.Button(button_frame, text="🔍 检查数据完整性", 
                                       command=self.check_data_completeness)
        self.check_data_btn.pack(fill=tk.X, pady=2)
        
        # 风险监控按钮
        self.crisis_monitor_btn = ttk.Button(button_frame, text="🚨 宏观危机监测", 
                                           command=self.run_crisis_monitor)
        self.crisis_monitor_btn.pack(fill=tk.X, pady=2)
        
        self.risk_dashboard_btn = ttk.Button(button_frame, text="📊 日度风险面板", 
                                           command=self.run_risk_dashboard)
        self.risk_dashboard_btn.pack(fill=tk.X, pady=2)
        
        self.combined_report_btn = ttk.Button(button_frame, text="📈 综合风险报告", 
                                            command=self.run_combined_report)
        self.combined_report_btn.pack(fill=tk.X, pady=2)
        
        # 原有功能按钮
        self.run_daily_btn = ttk.Button(button_frame, text="运行每日分析", 
                                       command=self.run_daily_analysis)
        self.run_daily_btn.pack(fill=tk.X, pady=2)
        
        self.list_factors_btn = ttk.Button(button_frame, text="列出所有因子", 
                                          command=self.list_factors)
        self.list_factors_btn.pack(fill=tk.X, pady=2)
        
        self.backfill_btn = ttk.Button(button_frame, text="历史数据回填", 
                                      command=self.show_backfill_dialog)
        self.backfill_btn.pack(fill=tk.X, pady=2)
        
        self.explain_btn = ttk.Button(button_frame, text="生成解读报告", 
                                     command=self.show_explain_dialog)
        self.explain_btn.pack(fill=tk.X, pady=2)
        
        self.test_btn = ttk.Button(button_frame, text="系统测试", 
                                  command=self.run_system_test)
        self.test_btn.pack(fill=tk.X, pady=2)
        
        # 因子操作区域
        factor_frame = ttk.LabelFrame(control_frame, text="单个因子操作", padding="5")
        factor_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 因子选择
        ttk.Label(factor_frame, text="选择因子:").pack(anchor=tk.W)
        self.factor_var = tk.StringVar()
        self.factor_combo = ttk.Combobox(factor_frame, textvariable=self.factor_var, 
                                        state="readonly", width=20)
        self.factor_combo.pack(fill=tk.X, pady=(0, 5))
        
        # 因子操作按钮
        factor_btn_frame = ttk.Frame(factor_frame)
        factor_btn_frame.pack(fill=tk.X)
        
        self.run_factor_btn = ttk.Button(factor_btn_frame, text="运行因子", 
                                        command=self.run_single_factor)
        self.run_factor_btn.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 2))
        
        self.analyze_factor_btn = ttk.Button(factor_btn_frame, text="详细分析", 
                                           command=self.analyze_single_factor)
        self.analyze_factor_btn.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(2, 0))
        
        # 因子历史数据按钮
        self.factor_history_btn = ttk.Button(factor_frame, text="获取历史数据", 
                                           command=self.get_factor_history)
        self.factor_history_btn.pack(fill=tk.X, pady=2)
        
        # 配置按钮
        config_frame = ttk.LabelFrame(control_frame, text="配置", padding="5")
        config_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.api_key_var = tk.StringVar(value=os.getenv("FRED_API_KEY", ""))
        ttk.Label(config_frame, text="FRED API密钥:").pack(anchor=tk.W)
        api_key_entry = ttk.Entry(config_frame, textvariable=self.api_key_var, show="*")
        api_key_entry.pack(fill=tk.X, pady=(0, 5))
        
        self.base_dir_var = tk.StringVar(value=os.getenv("MACROLAB_BASE_DIR", "D:\\MacroLab"))
        ttk.Label(config_frame, text="数据目录:").pack(anchor=tk.W)
        dir_frame = ttk.Frame(config_frame)
        dir_frame.pack(fill=tk.X)
        ttk.Entry(dir_frame, textvariable=self.base_dir_var).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(dir_frame, text="浏览", command=self.browse_directory).pack(side=tk.RIGHT, padx=(5, 0))
        
        # 右侧结果显示
        result_frame = ttk.LabelFrame(main_frame, text="运行结果", padding="10")
        result_frame.grid(row=1, column=1, rowspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(10, 0))
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)
        
        # 结果显示区域
        self.result_text = scrolledtext.ScrolledText(result_frame, height=20, width=60, 
                                                    font=('Consolas', 9))
        self.result_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 进度条
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        
        # 底部状态栏
        self.status_bar = ttk.Label(main_frame, text="就绪", relief=tk.SUNKEN)
        self.status_bar.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
    
    def initialize_system(self):
        """初始化系统"""
        def init_thread():
            try:
                self.log_message("正在初始化系统...")
                
                # 重新加载环境变量以确保最新值
                load_dotenv('macrolab.env', override=True)
                
                # 更新API密钥变量
                env_api_key = os.getenv("FRED_API_KEY", "")
                if env_api_key and not self.api_key_var.get():
                    self.api_key_var.set(env_api_key)
                    self.log_message(f"从环境变量加载FRED API密钥: {env_api_key[:8]}...")
                
                # 创建缓存管理器
                self.cache_manager = CacheManager(self.base_dir_var.get())
                
                # 创建FRED客户端
                api_key = self.api_key_var.get()
                if api_key:
                    self.fred_client = FredClient(api_key, self.cache_manager)
                    self.log_message("FRED客户端初始化成功")
                else:
                    self.log_message("警告: 未设置FRED API密钥")
                
                # 加载配置 - 使用默认配置如果文件不存在
                try:
                    self.settings = load_yaml_config("config/settings.yaml")
                except:
                    self.settings = {"outputs": {"write_excel": True}}
                    self.log_message("使用默认设置配置")
                
                try:
                    self.registry = FactorRegistry("factors", "config/factor_registry.yaml")
                except:
                    self.registry = None
                    self.log_message("因子注册表不可用，跳过因子相关功能")
                
                # 创建聚合器 - 只有在有FRED客户端时才创建
                if hasattr(self, 'fred_client') and self.fred_client:
                    try:
                        self.aggregator = DataAggregator(self.fred_client, self.cache_manager, 
                                                       self.registry, self.settings)
                    except:
                        self.aggregator = None
                        self.log_message("数据聚合器初始化失败，跳过聚合功能")
                else:
                    self.aggregator = None
                
                # 创建报告生成器
                try:
                    self.report_generator = ReportGenerator(self.base_dir_var.get())
                except:
                    self.report_generator = None
                    self.log_message("报告生成器初始化失败，跳过报告功能")
                
                # 加载因子列表到下拉框 - 只有在有注册表时才加载
                if self.registry:
                    try:
                        self.load_factor_list()
                    except:
                        self.log_message("因子列表加载失败")
                
                self.status_label.config(text="系统就绪", foreground="green")
                self.log_message("系统初始化完成！")
                
            except Exception as e:
                self.status_label.config(text="初始化失败", foreground="red")
                self.log_message(f"初始化失败: {e}")
        
        # 在后台线程中初始化
        threading.Thread(target=init_thread, daemon=True).start()
    
    def log_message(self, message):
        """添加日志消息"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.result_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.result_text.see(tk.END)
        self.root.update_idletasks()
    
    def run_daily_analysis(self):
        """运行每日分析"""
        if self.is_running:
            messagebox.showwarning("警告", "系统正在运行中，请稍候...")
            return
        
        def run_thread():
            try:
                self.is_running = True
                self.progress.start()
                self.run_daily_btn.config(state='disabled')
                
                self.log_message("开始运行每日分析...")
                
                # 检查聚合器是否可用
                if not hasattr(self, 'aggregator') or self.aggregator is None:
                    self.log_message("❌ 数据聚合器不可用，请检查系统配置")
                    return
                
                # 运行分析
                result = self.aggregator.run_daily_analysis()
                
                # 显示结果
                self.log_message(f"分析完成！")
                self.log_message(f"分析日期: {result['date'].strftime('%Y-%m-%d')}")
                self.log_message(f"综合风险评分: {result['total_score']:.2f}")
                self.log_message(f"风险等级: {result['risk_level']}")
                self.log_message("")
                self.log_message("各因子评分:")
                
                for factor_id, score in result['factor_scores'].items():
                    value = result['factor_values'].get(factor_id, "N/A")
                    if isinstance(value, float):
                        value_str = f"{value:.4f}"
                    else:
                        value_str = str(value)
                    self.log_message(f"  {factor_id}: {score:.2f} (值: {value_str})")
                
                # 生成报告
                if hasattr(self, 'report_generator') and self.report_generator:
                    try:
                        recent_scores = self.aggregator.get_recent_scores(5)
                        report_path = self.report_generator.generate_daily_report(result, recent_scores)
                        self.log_message(f"")
                        self.log_message(f"报告已生成: {report_path}")
                        
                        # 生成Excel汇总
                        if self.settings.get("outputs", {}).get("write_excel", True):
                            excel_path = os.getenv("MACROLAB_EXCEL_OUT", "D:\\标普\\backtest_results\\宏观金融危机风险打分系统.xlsx")
                            self.report_generator.generate_excel_summary(result, excel_path)
                            self.log_message(f"Excel报告已生成: {excel_path}")
                    except Exception as e:
                        self.log_message(f"⚠️ 报告生成失败: {e}")
                else:
                    self.log_message("⚠️ 报告生成器不可用，跳过报告生成")
                
                self.log_message("每日分析完成！")
                
            except Exception as e:
                self.log_message(f"每日分析失败: {e}")
                messagebox.showerror("错误", f"每日分析失败: {e}")
            finally:
                self.is_running = False
                self.progress.stop()
                self.run_daily_btn.config(state='normal')
        
        threading.Thread(target=run_thread, daemon=True).start()
    
    def list_factors(self):
        """列出所有因子"""
        try:
            self.log_message("正在获取因子列表...")
            
            factors = self.registry.list_factors()
            
            self.log_message(f"发现 {len(factors)} 个因子:")
            self.log_message("")
            
            for factor in factors:
                self.log_message(f"ID: {factor['id']}")
                self.log_message(f"名称: {factor['name']}")
                self.log_message(f"单位: {factor['units']}")
                self.log_message("-" * 40)
            
        except Exception as e:
            self.log_message(f"获取因子列表失败: {e}")
            messagebox.showerror("错误", f"获取因子列表失败: {e}")
    
    def show_backfill_dialog(self):
        """显示历史数据回填对话框"""
        dialog = BackfillDialog(self.root, self)
        self.root.wait_window(dialog.dialog)
    
    def show_explain_dialog(self):
        """显示解读报告对话框"""
        dialog = ExplainDialog(self.root, self)
        self.root.wait_window(dialog.dialog)
    
    def run_system_test(self):
        """运行系统测试"""
        if self.is_running:
            messagebox.showwarning("警告", "系统正在运行中，请稍候...")
            return
        
        def test_thread():
            try:
                self.is_running = True
                self.progress.start()
                self.test_btn.config(state='disabled')
                
                self.log_message("开始系统测试...")
                
                # 测试FRED连接
                if self.fred_client:
                    self.log_message("测试FRED API连接...")
                    vix_data = self.fred_client.get_series("VIXCLS")
                    if not vix_data.empty:
                        self.log_message(f"✓ FRED连接成功，获取到{len(vix_data)}条VIX数据")
                        self.log_message(f"最新VIX值: {vix_data.iloc[-1]:.2f}")
                    else:
                        self.log_message("✗ FRED连接失败")
                else:
                    self.log_message("✗ FRED客户端未初始化")
                
                # 测试因子注册表
                self.log_message("测试因子注册表...")
                factors = self.registry.get_all_factors()
                self.log_message(f"✓ 发现{len(factors)}个因子")
                
                # 测试评分算法
                self.log_message("测试评分算法...")
                from core.scoring import risk_score
                score = risk_score(20, 10, 30, reverse=False)
                self.log_message(f"✓ 评分算法测试通过: {score:.2f}")
                
                self.log_message("系统测试完成！")
                
            except Exception as e:
                self.log_message(f"系统测试失败: {e}")
                messagebox.showerror("错误", f"系统测试失败: {e}")
            finally:
                self.is_running = False
                self.progress.stop()
                self.test_btn.config(state='normal')
        
        threading.Thread(target=test_thread, daemon=True).start()
    
    def browse_directory(self):
        """浏览目录"""
        directory = filedialog.askdirectory(initialdir=self.base_dir_var.get())
        if directory:
            self.base_dir_var.set(directory)
    
    def load_factor_list(self):
        """加载因子列表到下拉框"""
        try:
            factors = self.registry.list_factors()
            factor_list = [f"{factor['id']} - {factor['name']}" for factor in factors]
            self.factor_combo['values'] = factor_list
            if factor_list:
                self.factor_combo.set(factor_list[0])
            self.log_message(f"加载了 {len(factor_list)} 个因子到选择列表")
        except Exception as e:
            self.log_message(f"加载因子列表失败: {e}")
    
    def run_single_factor(self):
        """运行单个因子"""
        if self.is_running:
            messagebox.showwarning("警告", "系统正在运行中，请稍候...")
            return
        
        selected_factor = self.factor_var.get()
        if not selected_factor:
            messagebox.showwarning("警告", "请先选择一个因子")
            return
        
        # 提取因子ID
        factor_id = selected_factor.split(" - ")[0]
        
        def run_thread():
            try:
                self.is_running = True
                self.progress.start()
                self.run_factor_btn.config(state='disabled')
                
                self.log_message(f"开始运行因子: {factor_id}")
                
                # 获取因子实例
                factor = self.registry.get_factor(factor_id)
                if not factor:
                    self.log_message(f"错误: 未找到因子 {factor_id}")
                    return
                
                # 获取数据
                self.log_message(f"正在获取 {factor.name} 数据...")
                df = factor.fetch()
                
                if df.empty:
                    # 尝试通过FRED客户端获取数据
                    if self.fred_client and hasattr(factor, 'series_id'):
                        series_id = getattr(factor, 'series_id', None)
                        if series_id:
                            self.log_message(f"通过FRED API获取数据: {series_id}")
                            df = self.fred_client.get_series_cached(series_id)
                            if not df.empty:
                                df = df.rename("value").to_frame().reset_index(names="date").dropna()
                
                if df.empty:
                    self.log_message(f"警告: {factor.name} 无可用数据")
                    return
                
                self.log_message(f"获取到 {len(df)} 条数据")
                self.log_message(f"数据范围: {df['date'].min()} 到 {df['date'].max()}")
                
                # 计算指标
                self.log_message("正在计算指标...")
                metrics = factor.compute(df)
                
                # 计算评分
                self.log_message("正在计算风险评分...")
                score = factor.score(metrics, self.settings)
                
                # 显示结果
                self.log_message("")
                self.log_message(f"=== {factor.name} 分析结果 ===")
                self.log_message(f"因子ID: {factor.id}")
                self.log_message(f"因子名称: {factor.name}")
                self.log_message(f"单位: {factor.units or 'N/A'}")
                self.log_message("")
                self.log_message("计算指标:")
                for key, value in metrics.items():
                    if isinstance(value, float):
                        self.log_message(f"  {key}: {value:.6f}")
                    else:
                        self.log_message(f"  {key}: {value}")
                self.log_message("")
                self.log_message(f"风险评分: {score:.2f}")
                
                # 获取风险等级
                from core.scoring import get_risk_level
                risk_level = get_risk_level(score, self.settings.get("risk_thresholds", {}))
                self.log_message(f"风险等级: {risk_level}")
                
                # 显示最新数据
                if not df.empty:
                    latest_data = df.iloc[-1]
                    self.log_message("")
                    self.log_message("最新数据:")
                    self.log_message(f"  日期: {latest_data['date']}")
                    self.log_message(f"  值: {latest_data['value']:.6f}")
                
                self.log_message(f"{factor.name} 分析完成！")
                
            except Exception as e:
                self.log_message(f"运行因子失败: {e}")
                messagebox.showerror("错误", f"运行因子失败: {e}")
            finally:
                self.is_running = False
                self.progress.stop()
                self.run_factor_btn.config(state='normal')
        
        threading.Thread(target=run_thread, daemon=True).start()
    
    def analyze_single_factor(self):
        """详细分析单个因子"""
        if self.is_running:
            messagebox.showwarning("警告", "系统正在运行中，请稍候...")
            return
        
        selected_factor = self.factor_var.get()
        if not selected_factor:
            messagebox.showwarning("警告", "请先选择一个因子")
            return
        
        # 提取因子ID
        factor_id = selected_factor.split(" - ")[0]
        
        def analyze_thread():
            try:
                self.is_running = True
                self.progress.start()
                self.analyze_factor_btn.config(state='disabled')
                
                self.log_message(f"开始详细分析因子: {factor_id}")
                
                # 获取因子实例
                factor = self.registry.get_factor(factor_id)
                if not factor:
                    self.log_message(f"错误: 未找到因子 {factor_id}")
                    return
                
                # 获取历史数据
                self.log_message(f"正在获取 {factor.name} 历史数据...")
                df = factor.fetch()
                
                if df.empty:
                    # 尝试通过FRED客户端获取数据
                    if self.fred_client and hasattr(factor, 'series_id'):
                        series_id = getattr(factor, 'series_id', None)
                        if series_id:
                            self.log_message(f"通过FRED API获取历史数据: {series_id}")
                            df = self.fred_client.get_series_cached(series_id)
                            if not df.empty:
                                df = df.rename("value").to_frame().reset_index(names="date").dropna()
                
                if df.empty:
                    self.log_message(f"警告: {factor.name} 无可用数据")
                    return
                
                self.log_message(f"获取到 {len(df)} 条历史数据")
                
                # 详细统计分析
                self.log_message("")
                self.log_message(f"=== {factor.name} 详细分析 ===")
                
                # 基本统计
                values = df['value'].dropna()
                if not values.empty:
                    self.log_message("基本统计:")
                    self.log_message(f"  数据点数: {len(values)}")
                    self.log_message(f"  平均值: {values.mean():.6f}")
                    self.log_message(f"  中位数: {values.median():.6f}")
                    self.log_message(f"  标准差: {values.std():.6f}")
                    self.log_message(f"  最小值: {values.min():.6f}")
                    self.log_message(f"  最大值: {values.max():.6f}")
                    self.log_message(f"  变异系数: {(values.std() / values.mean() * 100):.2f}%")
                
                # 时间范围
                self.log_message("")
                self.log_message("时间范围:")
                self.log_message(f"  开始日期: {df['date'].min()}")
                self.log_message(f"  结束日期: {df['date'].max()}")
                self.log_message(f"  时间跨度: {(df['date'].max() - df['date'].min()).days} 天")
                
                # 最近趋势
                if len(values) >= 5:
                    recent_5 = values.tail(5)
                    recent_10 = values.tail(10) if len(values) >= 10 else values
                    
                    self.log_message("")
                    self.log_message("最近趋势:")
                    self.log_message(f"  最近5天平均值: {recent_5.mean():.6f}")
                    self.log_message(f"  最近10天平均值: {recent_10.mean():.6f}")
                    
                    # 计算变化率
                    if len(values) >= 2:
                        prev_value = float(values.iloc[-2])
                        change_1d = ((values.iloc[-1] - prev_value) / prev_value * 100) if prev_value != 0 else 0
                        self.log_message(f"  1日变化率: {change_1d:.2f}%")
                    
                    if len(values) >= 6:
                        prev_value_5d = float(values.iloc[-6])
                        change_5d = ((values.iloc[-1] - prev_value_5d) / prev_value_5d * 100) if prev_value_5d != 0 else 0
                        self.log_message(f"  5日变化率: {change_5d:.2f}%")
                
                # 计算当前指标和评分
                self.log_message("")
                self.log_message("当前分析:")
                metrics = factor.compute(df)
                score = factor.score(metrics, self.settings)
                
                self.log_message("计算指标:")
                for key, value in metrics.items():
                    if isinstance(value, float):
                        self.log_message(f"  {key}: {value:.6f}")
                    else:
                        self.log_message(f"  {key}: {value}")
                
                self.log_message(f"风险评分: {score:.2f}")
                
                # 获取风险等级
                from core.scoring import get_risk_level
                risk_level = get_risk_level(score, self.settings.get("risk_thresholds", {}))
                self.log_message(f"风险等级: {risk_level}")
                
                # 评分区间信息
                bands = self.settings.get("bands", {}).get(factor_id, [])
                if bands:
                    self.log_message("")
                    self.log_message("评分区间配置:")
                    if len(bands) >= 2:
                        self.log_message(f"  低风险阈值: {bands[0]}")
                        self.log_message(f"  高风险阈值: {bands[1]}")
                        if len(bands) >= 3 and bands[2] == "reverse":
                            self.log_message("  评分方式: 反向评分")
                        else:
                            self.log_message("  评分方式: 正向评分")
                
                self.log_message(f"{factor.name} 详细分析完成！")
                
            except Exception as e:
                self.log_message(f"详细分析失败: {e}")
                messagebox.showerror("错误", f"详细分析失败: {e}")
            finally:
                self.is_running = False
                self.progress.stop()
                self.analyze_factor_btn.config(state='normal')
        
        threading.Thread(target=analyze_thread, daemon=True).start()
    
    def get_factor_history(self):
        """获取因子历史数据"""
        if self.is_running:
            messagebox.showwarning("警告", "系统正在运行中，请稍候...")
            return
        
        selected_factor = self.factor_var.get()
        if not selected_factor:
            messagebox.showwarning("警告", "请先选择一个因子")
            return
        
        # 提取因子ID
        factor_id = selected_factor.split(" - ")[0]
        
        def history_thread():
            try:
                self.is_running = True
                self.progress.start()
                self.factor_history_btn.config(state='disabled')
                
                self.log_message(f"开始获取因子历史数据: {factor_id}")
                
                # 获取因子实例
                factor = self.registry.get_factor(factor_id)
                if not factor:
                    self.log_message(f"错误: 未找到因子 {factor_id}")
                    return
                
                # 获取历史数据
                self.log_message(f"正在获取 {factor.name} 历史数据...")
                df = factor.fetch()
                
                if df.empty:
                    # 尝试通过FRED客户端获取数据
                    if self.fred_client and hasattr(factor, 'series_id'):
                        series_id = getattr(factor, 'series_id', None)
                        if series_id:
                            self.log_message(f"通过FRED API获取历史数据: {series_id}")
                            df = self.fred_client.get_series_cached(series_id)
                            if not df.empty:
                                df = df.rename("value").to_frame().reset_index(names="date").dropna()
                
                if df.empty:
                    self.log_message(f"警告: {factor.name} 无可用数据")
                    return
                
                self.log_message(f"获取到 {len(df)} 条历史数据")
                
                # 显示历史数据摘要
                self.log_message("")
                self.log_message(f"=== {factor.name} 历史数据摘要 ===")
                self.log_message(f"数据范围: {df['date'].min()} 到 {df['date'].max()}")
                self.log_message(f"数据点数: {len(df)}")
                
                # 显示最近10条数据
                self.log_message("")
                self.log_message("最近10条数据:")
                self.log_message("日期\t\t值")
                self.log_message("-" * 40)
                
                recent_data = df.tail(10)
                for _, row in recent_data.iterrows():
                    date_str = row['date'].strftime('%Y-%m-%d')
                    value_str = f"{row['value']:.6f}"
                    self.log_message(f"{date_str}\t{value_str}")
                
                # 保存数据到文件
                try:
                    import pandas as pd
                    filename = f"{factor_id}_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    filepath = os.path.join(self.base_dir_var.get(), "data", "raw", filename)
                    df.to_csv(filepath, index=False, encoding='utf-8-sig')
                    self.log_message(f"")
                    self.log_message(f"历史数据已保存到: {filepath}")
                except Exception as e:
                    self.log_message(f"保存数据失败: {e}")
                
                self.log_message(f"{factor.name} 历史数据获取完成！")
                
            except Exception as e:
                self.log_message(f"获取历史数据失败: {e}")
                messagebox.showerror("错误", f"获取历史数据失败: {e}")
            finally:
                self.is_running = False
                self.progress.stop()
                self.factor_history_btn.config(state='normal')
        
        threading.Thread(target=history_thread, daemon=True).start()
    
    def run_crisis_monitor(self):
        """运行宏观危机监测系统"""
        if self.is_running:
            messagebox.showwarning("警告", "系统正在运行中，请稍候...")
            return
        
        def monitor_thread():
            try:
                self.is_running = True
                self.progress.start()
                self.crisis_monitor_btn.config(state='disabled')
                
                self.log_message("🚨 启动宏观危机监测系统...")
                self.log_message("=" * 50)
                
                # 运行危机监测系统
                import subprocess
                import sys
                
                crisis_monitor_path = os.path.join(os.path.dirname(__file__), "crisis_monitor.py")
                
                if os.path.exists(crisis_monitor_path):
                    self.log_message("📊 正在运行宏观危机监测...")
                    self.log_message("⏳ 预计等待时间: 3-5分钟...")
                    
                    # 使用实时输出显示进度
                    process = subprocess.Popen([sys.executable, crisis_monitor_path],
                                             stdout=subprocess.PIPE, 
                                             stderr=subprocess.STDOUT,
                                             text=True, 
                                             bufsize=1,
                                             universal_newlines=True,
                                             encoding='utf-8',
                                             errors='replace')
                    
                    # 实时显示输出
                    while True:
                        output = process.stdout.readline()
                        if output == '' and process.poll() is not None:
                            break
                        if output and output.strip():
                            line = output.strip()
                            if line:
                                # 显示关键进度信息
                                if any(keyword in line for keyword in ['启动', '步骤', '完成', '生成', '保存', '报告']):
                                    self.log_message(line)
                    
                    # 等待进程完成
                    return_code = process.poll()
                    
                    if return_code == 0:
                        self.log_message("✅ 宏观危机监测完成!")
                        
                        # 查找生成的报告文件
                        outputs_dir = os.path.join(os.path.dirname(__file__), "outputs", "crisis_monitor")
                        if os.path.exists(outputs_dir):
                            import glob
                            latest_files = glob.glob(os.path.join(outputs_dir, "crisis_report_*.html"))
                            if latest_files:
                                latest_file = max(latest_files, key=os.path.getctime)
                                self.log_message(f"📄 报告已生成: {latest_file}")
                            
                            latest_images = glob.glob(os.path.join(outputs_dir, "crisis_report_long_*.png"))
                            if latest_images:
                                latest_image = max(latest_images, key=os.path.getctime)
                                self.log_message(f"🖼️ 长图已生成: {latest_image}")
                    else:
                        self.log_message(f"⚠️ 宏观危机监测警告，返回码: {return_code}")
                else:
                    self.log_message(f"❌ 找不到危机监测程序: {crisis_monitor_path}")
                
            except Exception as e:
                self.log_message(f"❌ 宏观危机监测失败: {e}")
                messagebox.showerror("错误", f"宏观危机监测失败: {e}")
            finally:
                self.is_running = False
                self.progress.stop()
                self.crisis_monitor_btn.config(state='normal')
        
        threading.Thread(target=monitor_thread, daemon=True).start()
    
    def run_risk_dashboard(self):
        """运行日度风险面板系统"""
        if self.is_running:
            messagebox.showwarning("警告", "系统正在运行中，请稍候...")
            return
        
        def dashboard_thread():
            try:
                self.is_running = True
                self.progress.start()
                self.risk_dashboard_btn.config(state='disabled')
                
                self.log_message("📊 启动日度风险面板系统...")
                self.log_message("=" * 50)
                
                # 运行风险面板系统
                import subprocess
                import sys
                
                risk_dashboard_path = os.path.join(os.path.dirname(__file__), "daily_risk_dashboard", "risk_dashboard.py")
                
                if os.path.exists(risk_dashboard_path):
                    self.log_message("🚨 正在运行日度风险监控...")
                    self.log_message("⏳ 预计等待时间: 1-2分钟...")
                    
                    # 使用实时输出显示进度
                    process = subprocess.Popen([sys.executable, risk_dashboard_path],
                                             stdout=subprocess.PIPE, 
                                             stderr=subprocess.STDOUT,
                                             text=True, 
                                             bufsize=1,
                                             universal_newlines=True,
                                             encoding='utf-8',
                                             errors='replace')
                    
                    # 实时显示输出
                    while True:
                        output = process.stdout.readline()
                        if output == '' and process.poll() is not None:
                            break
                        if output and output.strip():
                            line = output.strip()
                            if line:
                                # 显示关键进度信息
                                if any(keyword in line for keyword in ['启动', '处理', '计算', '生成', '保存', '完成']):
                                    self.log_message(line)
                    
                    # 等待进程完成
                    return_code = process.poll()
                    
                    if return_code == 0:
                        self.log_message("✅ 日度风险面板生成完成!")
                        
                        # 查找生成的报告文件
                        outputs_dir = os.path.join(os.path.dirname(__file__), "daily_risk_dashboard", "outputs")
                        if os.path.exists(outputs_dir):
                            import glob
                            latest_images = glob.glob(os.path.join(outputs_dir, "risk_dashboard_*.png"))
                            if latest_images:
                                latest_image = max(latest_images, key=os.path.getctime)
                                self.log_message(f"🖼️ 风险面板已生成: {latest_image}")
                            
                            latest_json = glob.glob(os.path.join(outputs_dir, "risk_dashboard_*.json"))
                            if latest_json:
                                latest_data = max(latest_json, key=os.path.getctime)
                                self.log_message(f"📊 数据文件已生成: {latest_data}")
                    else:
                        self.log_message(f"⚠️ 日度风险面板警告，返回码: {return_code}")
                else:
                    self.log_message(f"❌ 找不到风险面板程序: {risk_dashboard_path}")
                
            except Exception as e:
                self.log_message(f"❌ 日度风险面板失败: {e}")
                messagebox.showerror("错误", f"日度风险面板失败: {e}")
            finally:
                self.is_running = False
                self.progress.stop()
                self.risk_dashboard_btn.config(state='normal')
        
        threading.Thread(target=dashboard_thread, daemon=True).start()
    
    def run_combined_report(self):
        """运行综合风险报告"""
        if self.is_running:
            messagebox.showwarning("警告", "系统正在运行中，请稍候...")
            return
        
        def combined_thread():
            try:
                self.is_running = True
                self.progress.start()
                self.combined_report_btn.config(state='disabled')
                
                self.log_message("📈 启动综合风险报告生成...")
                self.log_message("=" * 50)
                
                # 先运行宏观危机监测
                self.log_message("1️⃣ 运行宏观危机监测...")
                crisis_monitor_path = os.path.join(os.path.dirname(__file__), "crisis_monitor.py")
                
                if os.path.exists(crisis_monitor_path):
                    import subprocess
                    import sys
                    
                    # 使用实时输出显示进度
                    process1 = subprocess.Popen([sys.executable, crisis_monitor_path],
                                             stdout=subprocess.PIPE, 
                                             stderr=subprocess.STDOUT,
                                             text=True, 
                                             bufsize=1,
                                             universal_newlines=True,
                                             encoding='utf-8',
                                             errors='replace')
                    
                    # 实时显示输出
                    while True:
                        output = process1.stdout.readline()
                        if output == '' and process1.poll() is not None:
                            break
                        if output and output.strip():
                            line = output.strip()
                            if line and any(keyword in line for keyword in ['启动', '步骤', '完成', '生成', '保存', '报告']):
                                self.log_message(f"  {line}")
                    
                    return_code1 = process1.poll()
                    
                    if return_code1 == 0:
                        self.log_message("✅ 宏观危机监测完成")
                    else:
                        self.log_message("⚠️ 宏观危机监测有警告")
                
                # 再运行日度风险面板
                self.log_message("2️⃣ 运行日度风险面板...")
                risk_dashboard_path = os.path.join(os.path.dirname(__file__), "daily_risk_dashboard", "risk_dashboard.py")
                
                if os.path.exists(risk_dashboard_path):
                    # 使用实时输出显示进度
                    process2 = subprocess.Popen([sys.executable, risk_dashboard_path],
                                             stdout=subprocess.PIPE, 
                                             stderr=subprocess.STDOUT,
                                             text=True, 
                                             bufsize=1,
                                             universal_newlines=True,
                                             encoding='utf-8',
                                             errors='replace')
                    
                    # 实时显示输出
                    while True:
                        output = process2.stdout.readline()
                        if output == '' and process2.poll() is not None:
                            break
                        if output and output.strip():
                            line = output.strip()
                            if line and any(keyword in line for keyword in ['启动', '处理', '计算', '生成', '保存', '完成']):
                                self.log_message(f"  {line}")
                    
                    return_code2 = process2.poll()
                    
                    if return_code2 == 0:
                        self.log_message("✅ 日度风险面板完成")
                    else:
                        self.log_message("⚠️ 日度风险面板有警告")
                
                # 生成综合报告
                self.log_message("3️⃣ 生成综合风险报告...")
                self.generate_combined_summary()
                
                self.log_message("🎉 综合风险报告生成完成!")
                
            except Exception as e:
                self.log_message(f"❌ 综合风险报告失败: {e}")
                messagebox.showerror("错误", f"综合风险报告失败: {e}")
            finally:
                self.is_running = False
                self.progress.stop()
                self.combined_report_btn.config(state='normal')
        
        threading.Thread(target=combined_thread, daemon=True).start()
    
    def generate_combined_summary(self):
        """生成综合风险摘要"""
        try:
            import json
            import glob
            from datetime import datetime
            
            # 读取最新的危机监测数据
            crisis_outputs = os.path.join(os.path.dirname(__file__), "outputs", "crisis_monitor")
            risk_outputs = os.path.join(os.path.dirname(__file__), "daily_risk_dashboard", "outputs")
            
            crisis_data = None
            risk_data = None
            
            # 读取危机监测JSON数据
            if os.path.exists(crisis_outputs):
                crisis_json_files = glob.glob(os.path.join(crisis_outputs, "crisis_report_*.json"))
                if crisis_json_files:
                    latest_crisis = max(crisis_json_files, key=os.path.getctime)
                    with open(latest_crisis, 'r', encoding='utf-8') as f:
                        crisis_data = json.load(f)
            
            # 读取风险面板JSON数据
            if os.path.exists(risk_outputs):
                risk_json_files = glob.glob(os.path.join(risk_outputs, "risk_dashboard_*.json"))
                if risk_json_files:
                    latest_risk = max(risk_json_files, key=os.path.getctime)
                    with open(latest_risk, 'r', encoding='utf-8') as f:
                        risk_data = json.load(f)
            
            # 生成综合摘要
            self.log_message("")
            self.log_message("📊 综合风险摘要")
            self.log_message("=" * 50)
            
            if crisis_data:
                crisis_score = crisis_data.get('total_score', 0)
                crisis_level = crisis_data.get('risk_level', '未知')
                self.log_message(f"🚨 宏观危机评分: {crisis_score:.1f}/100 ({crisis_level})")
            
            if risk_data:
                risk_score = risk_data.get('total_score', 0)
                risk_summary = risk_data.get('summary', {})
                high_risk_count = risk_summary.get('high_risk_count', 0)
                total_indicators = risk_summary.get('total_indicators', 0)
                self.log_message(f"📊 日度风险评分: {risk_score:.1f}/100")
                self.log_message(f"🔴 高风险指标: {high_risk_count}/{total_indicators}")
            
            # 综合建议
            self.log_message("")
            self.log_message("💡 综合建议:")
            
            if crisis_data and risk_data:
                crisis_score = crisis_data.get('total_score', 0)
                risk_score = risk_data.get('total_score', 0)
                
                if crisis_score >= 70 and risk_score >= 70:
                    self.log_message("🚨 双重高风险警告！建议大幅减仓")
                elif crisis_score >= 70 or risk_score >= 70:
                    self.log_message("⚠️ 单一高风险，建议适度减仓")
                elif crisis_score >= 50 or risk_score >= 50:
                    self.log_message("🟡 中等风险，保持谨慎")
                else:
                    self.log_message("🟢 风险可控，可考虑适度加仓")
            
            self.log_message("")
            self.log_message("📁 详细报告文件:")
            
            # 列出生成的文件
            if os.path.exists(crisis_outputs):
                crisis_files = glob.glob(os.path.join(crisis_outputs, "crisis_report_*.html"))
                if crisis_files:
                    latest_crisis = max(crisis_files, key=os.path.getctime)
                    self.log_message(f"  📄 宏观危机报告: {latest_crisis}")
            
            if os.path.exists(risk_outputs):
                risk_files = glob.glob(os.path.join(risk_outputs, "risk_dashboard_*.png"))
                if risk_files:
                    latest_risk = max(risk_files, key=os.path.getctime)
                    self.log_message(f"  🖼️ 日度风险面板: {latest_risk}")
            
        except Exception as e:
            self.log_message(f"⚠️ 生成综合摘要失败: {e}")
    
    def check_data_completeness(self):
        """检查数据完整性"""
        if self.is_running:
            messagebox.showwarning("警告", "系统正在运行中，请稍候...")
            return
        
        def check_thread():
            try:
                self.is_running = True
                self.progress.start()
                self.check_data_btn.config(state='disabled')
                
                self.log_message("🔍 开始检查数据完整性...")
                self.log_message("=" * 50)
                
                # 运行数据完整性检查
                import subprocess
                import sys
                
                check_script_path = os.path.join(os.path.dirname(__file__), "check_data_completeness.py")
                
                if os.path.exists(check_script_path):
                    # 使用实时输出显示进度
                    process = subprocess.Popen([sys.executable, check_script_path],
                                             stdout=subprocess.PIPE, 
                                             stderr=subprocess.STDOUT,
                                             text=True, 
                                             bufsize=1,
                                             universal_newlines=True,
                                             encoding='utf-8',
                                             errors='replace')
                    
                    # 实时显示输出
                    while True:
                        output = process.stdout.readline()
                        if output == '' and process.poll() is not None:
                            break
                        if output and output.strip():
                            line = output.strip()
                            if line:
                                self.log_message(line)
                    
                    # 等待进程完成
                    return_code = process.poll()
                    
                    if return_code == 0:
                        self.log_message("✅ 数据完整性检查完成!")
                    else:
                        self.log_message(f"⚠️ 数据检查警告，返回码: {return_code}")
                else:
                    self.log_message(f"❌ 找不到数据检查脚本: {check_script_path}")
                
            except Exception as e:
                self.log_message(f"❌ 数据完整性检查失败: {e}")
                messagebox.showerror("错误", f"数据完整性检查失败: {e}")
            finally:
                self.is_running = False
                self.progress.stop()
                self.check_data_btn.config(state='normal')
        
        threading.Thread(target=check_thread, daemon=True).start()
    
    def download_fred_data(self):
        """下载FRED数据"""
        if self.is_running:
            messagebox.showwarning("警告", "系统正在运行中，请稍候...")
            return
        
        def download_thread():
            try:
                self.is_running = True
                self.progress.start()
                self.download_data_btn.config(state='disabled')
                
                self.log_message("📥 开始下载FRED数据...")
                self.log_message("=" * 50)
                self.log_message("⏳ 预计等待时间: 5-10分钟...")
                
                # 运行FRED数据下载
                import subprocess
                import sys
                
                download_script_path = os.path.join(os.path.dirname(__file__), "scripts", "sync_fred_http.py")
                
                if os.path.exists(download_script_path):
                    # 使用实时输出显示进度
                    process = subprocess.Popen([sys.executable, download_script_path],
                                             stdout=subprocess.PIPE, 
                                             stderr=subprocess.STDOUT,
                                             text=True, 
                                             bufsize=1,
                                             universal_newlines=True,
                                             encoding='utf-8',
                                             errors='replace')
                    
                    # 实时显示输出
                    while True:
                        output = process.stdout.readline()
                        if output == '' and process.poll() is not None:
                            break
                        if output and output.strip():
                            # 过滤和格式化输出
                            line = output.strip()
                            if line:
                                # 显示关键进度信息
                                if any(keyword in line for keyword in ['开始同步', '同步完成', '处理进度', 'FRED数据同步完成', '计算', '完成']):
                                    self.log_message(line)
                                elif '✓' in line or '✗' in line:
                                    self.log_message(line)
                    
                    # 等待进程完成
                    return_code = process.poll()
                    
                    if return_code == 0:
                        self.log_message("✅ FRED数据下载完成!")
                    else:
                        self.log_message(f"⚠️ 数据下载警告，返回码: {return_code}")
                else:
                    self.log_message(f"❌ 找不到数据下载脚本: {download_script_path}")
                
                # 下载完成后自动检查数据完整性
                self.log_message("\n🔍 下载完成，自动检查数据完整性...")
                check_script_path = os.path.join(os.path.dirname(__file__), "check_data_completeness.py")
                
                if os.path.exists(check_script_path):
                    result2 = subprocess.run([sys.executable, check_script_path],
                                           capture_output=True, text=True, timeout=60)
                    
                    if result2.returncode == 0:
                        self.log_message("✅ 数据完整性检查完成!")
                        
                        # 显示检查结果摘要
                        if result2.stdout:
                            output_lines = result2.stdout.split('\n')
                        else:
                            output_lines = []
                        for line in output_lines:
                            if '数据完整性:' in line or '需要处理' in line or '操作建议' in line:
                                self.log_message(line)
                
            except Exception as e:
                self.log_message(f"❌ FRED数据下载失败: {e}")
                messagebox.showerror("错误", f"FRED数据下载失败: {e}")
            finally:
                self.is_running = False
                self.progress.stop()
                self.download_data_btn.config(state='normal')
        
        threading.Thread(target=download_thread, daemon=True).start()


class BackfillDialog:
    """历史数据回填对话框"""
    
    def __init__(self, parent, main_app):
        self.main_app = main_app
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("历史数据回填")
        self.dialog.geometry("400x200")
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # 居中显示
        self.dialog.geometry("+%d+%d" % (parent.winfo_rootx() + 50, parent.winfo_rooty() + 50))
        
        # 创建界面
        frame = ttk.Frame(self.dialog, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="历史数据回填", font=('Arial', 12, 'bold')).pack(pady=(0, 20))
        
        # 开始日期
        start_frame = ttk.Frame(frame)
        start_frame.pack(fill=tk.X, pady=5)
        ttk.Label(start_frame, text="开始日期:").pack(side=tk.LEFT)
        self.start_date = ttk.Entry(start_frame)
        self.start_date.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=(10, 0))
        self.start_date.insert(0, "2020-01-01")
        
        # 结束日期
        end_frame = ttk.Frame(frame)
        end_frame.pack(fill=tk.X, pady=5)
        ttk.Label(end_frame, text="结束日期:").pack(side=tk.LEFT)
        self.end_date = ttk.Entry(end_frame)
        self.end_date.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=(10, 0))
        self.end_date.insert(0, "2024-12-31")
        
        # 按钮
        button_frame = ttk.Frame(frame)
        button_frame.pack(fill=tk.X, pady=(20, 0))
        
        ttk.Button(button_frame, text="开始回填", command=self.start_backfill).pack(side=tk.LEFT)
        ttk.Button(button_frame, text="取消", command=self.dialog.destroy).pack(side=tk.RIGHT)
    
    def start_backfill(self):
        """开始回填"""
        start_date = self.start_date.get()
        end_date = self.end_date.get()
        
        try:
            # 验证日期格式
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
            
            self.main_app.log_message(f"开始历史数据回填: {start_date} 到 {end_date}")
            self.dialog.destroy()
            
            # 在后台线程中运行回填
            def backfill_thread():
                try:
                    self.main_app.aggregator.backfill(start_date, end_date)
                    self.main_app.log_message("历史数据回填完成！")
                except Exception as e:
                    self.main_app.log_message(f"历史数据回填失败: {e}")
            
            threading.Thread(target=backfill_thread, daemon=True).start()
            
        except ValueError:
            messagebox.showerror("错误", "日期格式错误，请使用 YYYY-MM-DD 格式")


class ExplainDialog:
    """解读报告对话框"""
    
    def __init__(self, parent, main_app):
        self.main_app = main_app
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("生成解读报告")
        self.dialog.geometry("400x200")
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # 居中显示
        self.dialog.geometry("+%d+%d" % (parent.winfo_rootx() + 50, parent.winfo_rooty() + 50))
        
        # 创建界面
        frame = ttk.Frame(self.dialog, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="生成解读报告", font=('Arial', 12, 'bold')).pack(pady=(0, 20))
        
        # 分析日期
        date_frame = ttk.Frame(frame)
        date_frame.pack(fill=tk.X, pady=5)
        ttk.Label(date_frame, text="分析日期:").pack(side=tk.LEFT)
        self.analysis_date = ttk.Entry(date_frame)
        self.analysis_date.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=(10, 0))
        self.analysis_date.insert(0, datetime.now().strftime("%Y-%m-%d"))
        
        # AI解读选项
        self.use_ai = tk.BooleanVar()
        ttk.Checkbutton(frame, text="使用AI解读", variable=self.use_ai).pack(pady=10)
        
        # 按钮
        button_frame = ttk.Frame(frame)
        button_frame.pack(fill=tk.X, pady=(20, 0))
        
        ttk.Button(button_frame, text="生成报告", command=self.generate_report).pack(side=tk.LEFT)
        ttk.Button(button_frame, text="取消", command=self.dialog.destroy).pack(side=tk.RIGHT)
    
    def generate_report(self):
        """生成报告"""
        analysis_date = self.analysis_date.get()
        use_ai = self.use_ai.get()
        
        try:
            # 验证日期格式
            datetime.strptime(analysis_date, "%Y-%m-%d")
            
            self.main_app.log_message(f"生成解读报告: {analysis_date}")
            self.dialog.destroy()
            
            # 在后台线程中生成报告
            def explain_thread():
                try:
                    target_date = datetime.strptime(analysis_date, "%Y-%m-%d")
                    result = self.main_app.aggregator.run_daily_analysis(target_date)
                    recent_scores = self.main_app.aggregator.get_recent_scores(5)
                    report_path = self.main_app.report_generator.generate_daily_report(result, recent_scores)
                    self.main_app.log_message(f"解读报告已生成: {report_path}")
                except Exception as e:
                    self.main_app.log_message(f"生成解读报告失败: {e}")
            
            threading.Thread(target=explain_thread, daemon=True).start()
            
        except ValueError:
            messagebox.showerror("错误", "日期格式错误，请使用 YYYY-MM-DD 格式")


def main():
    """主函数"""
    root = tk.Tk()
    app = MacroLabGUI(root)
    
    # 设置窗口图标（如果有的话）
    try:
        root.iconbitmap("icon.ico")
    except:
        pass
    
    root.mainloop()


if __name__ == "__main__":
    main()
